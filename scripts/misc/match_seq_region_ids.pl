#!/usr/bin/env perl

# Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
# Copyright [2016-2022] EMBL-European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use strict;
use warnings;

use Bio::EnsEMBL::Registry;

use DBI qw(:sql_types);
use Getopt::Long;

usage() if (!scalar(@ARGV));

my $config = {};

GetOptions(
  $config,
  'registry=s',
  'dry_run',
  'help!',
) or die "Error: Failed to parse command line arguments\n";

usage() if ($config->{help});

die ('A registry file is required (--registry)') unless (defined($config->{registry}));

my $registry = 'Bio::EnsEMBL::Registry';   
$registry->load_all($config->{registry});

my $cdbas = $registry->get_all_DBAdaptors(-group => 'core');
my $vdbas_tmp = $registry->get_all_DBAdaptors(-group => 'variation');
my $vdbas = {};
foreach my $vdba (@$vdbas_tmp) {
  my $species = $vdba->species;
  $vdbas->{$species} = $vdba;
}

foreach my $cdba (@$cdbas) {
  my $species = $cdba->species;
  die ("Species not found") if (!$vdbas->{$species});
  my $dbh = $cdba->dbc->db_handle;
  my $id_mapping = {};
  my $sth = $dbh->prepare("SELECT seq_region_id, name FROM seq_region;");
  $sth->execute();
  while (my @row = $sth->fetchrow_array) {
    my $external_seq_region_id = $row[0];
    my $external_seq_region_name = $row[1];
    $id_mapping->{$external_seq_region_name} = $external_seq_region_id;     
  }
  $sth->finish();
  
  my $vdba = $vdbas->{$species};
  my $vdbh = $vdba->dbc->db_handle;

  my $dbname = $vdba->dbc->dbname;
  my @vd_names = ();
  $sth = $vdbh->prepare("SELECT name FROM seq_region;");
  $sth->execute();
  while (my @row = $sth->fetchrow_array) {
    push(@vd_names, $row[0]);
  }
  $sth->finish();

  die ("Error: Core and Variation DB does not have same name sizes\nPlease check if new release has new seq_region names.\n")
  unless((scalar @vd_names) == (keys %$id_mapping));

  # Check if all Core DB seq_region name values are in Variation DB
  foreach my $prev_seq_region_name ( keys %$id_mapping) {
      die ( "Row value: '$prev_seq_region_name' is not listed in $dbname.seq_region name column\n" ) if ( ! grep $_ eq $prev_seq_region_name, @vd_names);
  }

  # Remove old seq_region_id from vdb and create a new one based on core
  unless (defined($config->{dry_run})) {
    $vdbh->do("ALTER TABLE seq_region drop seq_region_id") or die $dbh->errstr;
    $vdbh->do("ALTER TABLE seq_region ADD seq_region_id INT NOT NULL") or die $dbh->errstr;
  }

  foreach my $prev_seq_region_name ( keys %$id_mapping) {
    my $new_seq_region_id = $id_mapping->{$prev_seq_region_name};
    if ($config->{dry_run}) {
      print "For $dbname: Update seq_region SET seq_region_id=$new_seq_region_id WHERE name='$prev_seq_region_name'\n";
    } else {
      $vdbh->do("Update seq_region SET seq_region_id=$new_seq_region_id WHERE name='$prev_seq_region_name'") or die $dbh->errstr;
    }
  }
}

sub usage {
  print qq{
  Usage: perl update_seq_region_ids.pl -registry [registry_file] [OPTIONS]
  Update seq_region_ids between releases. Check if any seq_region_ids have changed since the last release and update them if needed. 
  Options:
    -help    Print this message
    -dry_run Print update statements
  } . "\n";
  exit(0);
}
