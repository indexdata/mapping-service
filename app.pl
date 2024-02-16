#! /usr/bin/perl

use strict;
use warnings;

use MARC::Record;
use MARC::Record::MiJ;
use JSON;
use UUID::Tiny ':std';
use MARC::Charset 'marc8_to_utf8';
use Time::Piece;
use File::Basename;
use HTTP::Server::Simple;
use LWP::UserAgent;
use HTTP::CookieJar::LWP;
use Mozilla::CA;
use Data::Dumper;

my $start = time();

sub getConfig {
  local $/ = '';
  open CONF, 'config.json' or die "Can't open config file!";
  my $j = decode_json(<CONF>);
  return $j;
}

my $conf = getConfig();
my $rules_file = 'mapping-rules/default.json';

my @refeps = ('contributor-name-types','contributor-types','alternative-title-types','classification-types','electronic-access-relationships',,'identifier-types','instance-formats','instance-note-types','instance-relationship-types','instance-statuses','instance-types','modes-of-issuance','nature-of-content-terms','statistical-code-types','statistical-codes','hrid-settings-storage/hrid-settings');

my @hrefeps = ('call-number-types','holdings-note-types','holdings-types','holdings-sources','ill-policies','item-damaged-statuses','item-note-types','loan-types','locations','material-types','service-points','shelf-locations',);

my $hrid_conf = {};
my $tenant = $conf->{tenant};
sub getRefData {
  my $refobj = {};
  my $okapi = $conf->{okapi};
  my $user = $conf->{username};
  my $pass = $conf->{password}; 
  my $url = "$okapi/authn/login-with-expiry";
  my @h = ('content-type' => 'application/json', 'x-okapi-tenant' => $tenant);
  my $pl = { username=>$user, password=>$pass };
  my $payload = encode_json($pl);
  my $jar = HTTP::CookieJar::LWP->new;
  my $ua = LWP::UserAgent->new(cookie_jar => $jar);
  my $res = $ua->post($url, @h, 'Content' => $payload);
  if (!$res->is_success) {
    die $res->decoded_content;
  }
  foreach (@refeps) {
    my $url = "$okapi/$_?limit=500";
    my $res = $ua->get($url);
    my $body = '';
    if ($res->is_success) {
      $body = $res->decoded_content;
    }
    my $json = eval { decode_json($body) };
    if ($@) {
      print "WARN $_ is not valid JSON!\n";
    } elsif ($url =~ /hrid-settings/) {
      my $curr = $json->{instances}->{currentNumber};
      my $pre = $json->{instances}->{prefix};
      $hrid_conf = { 
        inst => { cur => $curr, pre => $pre }
      };
    } else {
      foreach (keys %$json) {
        if ($_ ne 'totalRecords') {
          my $refroot = $_;
          $refobj->{$refroot} = {};
          foreach (@{ $json->{$_} }) {
            my $name;
            my $id = $_->{id};
            if ($refroot eq 'contributorTypes') {
              my $n = lc $_->{name};
              my $c = $_->{code};
              $refobj->{$refroot}->{$n} = $id;
              $refobj->{$refroot}->{$c} = $id;
              next;
            } elsif ($refroot =~ /^(instanceTypes|instanceFormats)$/) {
              $name = $_->{code};
            } else {
              $name = $_->{name};
            }
            $name =~ s/\s+$//;
            $refobj->{$refroot}->{$name} = $id;
          }
        }
      }
    }
  }
 return $refobj;
}
# print "Getting reference data...\n";
my $refdata = getRefData();
# print Dumper ($refdata); exit;
# print Dumper ($hrid_conf); exit;

my $ver = 1;
binmode STDOUT, ":utf8";
$| = 1;

my $srstype = 'MARC';
my $source_id = 'f32d531e-df79-46b3-8932-cdd35f7a2264'; # Folio 

if (! $ARGV[0]) {
  die "Usage: ./marc2inst-neco.pl  <raw_marc_files>\n";
}
my $dir = dirname($ARGV[0]);

my $json = JSON->new;
$json->canonical();

my @lt = localtime();
my $mdate = sprintf("%04d-%02d-%02dT%02d:%02d:%02d-0500", $lt[5] + 1900, $lt[4] + 1, $lt[3], $lt[2], $lt[1], $lt[0]);

sub uuid {
  my $text = shift;
  my $uuid = create_uuid_as_string(UUID_V5, $text . $tenant);
  return $uuid;
}

sub getRules {
  my $rfile = shift;
  local $/ = '';
  open my $rules, $rfile or die "Can't open $rfile";
  my $jsonstr = <$rules>;
  my $json = decode_json($jsonstr);
  return $json;
}

my $blvl = {
  'm' => 'Monograph',
  'i' => 'Integrating Resource',
  's' => 'Serial',
  'a' => 'single unit'
};

my $relations = {
  '0' => 'Resource',
  '1' => 'Version of resource',
  '2' => 'Related resource',
  '3' => 'No information provided'
};

my $pub_roles = {
  '0' => 'Production',
  '1' => 'Publication',
  '2' => 'Distribution',
  '3' => 'Manufacture',
  '4' => 'Copyright notice date'
};

my $rtypes = {
  'a' => 'txt',
  'c' => 'ntm',
  'd' => 'ntm',
  'e' => 'cri',
  'f' => 'cri',
  'm' => 'cop',
  'g' => 'tdi',
  'i' => 'spw',
  'j' => 'prm',
  'k' => 'sti',
  'o' => 'xxx',
  'p' => 'xxx',
  'r' => 'tdf',
  't' => 'txt'
};

my $typemap = {
  'x' => 'Monograph',
  'y' => 'Serial',
  'v' => 'Multi-part monograph',
  'u' => 'Monograph'
};

my $bcseen = {};
my $hrseen = {};

sub process_entity {
  my $field = shift;
  my $ent = shift;
  my @data;
  my $out;
  my @rules;
  if ($ent->{rules}) {
    @rules = @{ $ent->{rules} };
  }
  my @funcs;
  my $default;
  my $params;
  my $tag = $field->tag();
  my $func_type;
  my $subs;
  if ($ent->{subfield}) {
    $subs = join '', @{ $ent->{subfield} };
  }
  foreach (@rules) {
    foreach (@{ $_->{conditions} }) {
      $func_type = $_->{type};
      @funcs = split /,\s*/, $_->{type};
      $params = $_->{parameter};
    }
    $default = $_->{value};
  }
  if ($tag =~ /^00/) {
    my $d;
    if ($default) {
      $d = $default;
    } else {
      $d = $field->data();
    }
    push @data, $d;
    $ent->{applyRulesOnConcatenatedData} = JSON::true;
  } elsif ($default || ($func_type && $func_type =~ /\bset_/ && $params)) {
    my $add = 0;
    if (!$subs) {
      $add = 1;
    } else {
      foreach ($field->subfields()) {
        if ($subs =~ /\Q$_->[0]\E/ && $_->[1] =~ /\S/) {
          $add = 1;
          last;
        }
      }
    }
    if ($default) {
      push @data, $default if $add;
    } else {
      my $d = processing_funcs('', $field, $params, @funcs);
      push @data, $d if $add;
    }
  } else {
    my $tmp_field = $field->clone();
    if (!$ent->{applyRulesOnConcatenatedData}) {
      my $i = 0;
      my $sf;
      foreach (@{ $tmp_field->{_subfields} }) {
        if ($i % 2 && $subs =~ /\Q$sf\E/) {
          $_ = processing_funcs($_, $tmp_field, $params, @funcs);
        } else {
          $sf = $_;
        }
        $i++;
      }
    }
    if ($ent->{subFieldDelimiter}) {
      my @sects;
      my $del = ' ';
      foreach (@{ $ent->{subFieldDelimiter} }) {
        my $subs = join '', @{ $_->{subfields} };
        if ($subs) {
          my $sdata = $tmp_field->as_string($subs, $_->{value}); 
          push @sects, $sdata if $sdata;
        } else {
          $del = $_->{value};
        }
      }
      push @data, join $del, @sects;
    } else {
      push @data, $tmp_field->as_string($subs) if $subs;
    }
  }
  
  if ($data[0]) {
    $out = join ' ', @data;
    $out = processing_funcs($out, $field, $params, @funcs) if $ent->{applyRulesOnConcatenatedData};
  }
  return $out;
}

sub processing_funcs {
  my $out = shift || '';
  my $field = shift;
  my $params = shift;
  foreach (@_) {
    if ($_ eq 'trim_period') {
      $out =~ s/\.\s*$//;
    } elsif ($_ eq 'trim') {
      $out =~ s/^\s+|\s+$//g;
    } elsif ($_ eq 'remove_ending_punc') {
      $out =~ s/[;:,\/+= ]$//g;
    } elsif ($_ eq 'remove_prefix_by_indicator') {
      my $ind = $field->indicator(2);
      if ($ind eq ' ') {
        $ind = 0;
      }
      if ($ind > 0 && length($out) > $ind) {
        $out = substr($out, $ind);
      }
    } elsif ($_ eq 'set_identifier_type_id_by_name') {
      my $name = $params->{name};
      $out = $refdata->{identifierTypes}->{$name} || '2e8b3b6c-0e7d-4e48-bca2-b0b23b376af5' 
    } elsif ($_ eq 'set_contributor_name_type_id') {
      my $name = $params->{name};
      $out = $refdata->{contributorNameTypes}->{$name} or die "Can't find contributorNameType for $name";
    } elsif ($_ eq 'set_contributor_type_id') {
      $out = $refdata->{contributorTypes}->{$out} || '';
    } elsif ($_ eq 'set_contributor_type_id_by_code_or_name') {
      my $cc = $params->{contributorCodeSubfield};
      my $nc = $params->{contributorNameSubfield};
      my $ccode = $field->subfield($cc);
      my $cname = $field->subfield($nc);
      if ($ccode) {
        $out = $refdata->{contributorTypes}->{$ccode};
      } elsif ($cname && !$out) {
        $cname =~ s/[,.]//g;
        $out = $refdata->{contributorTypes}->{$cname};
      }
    } elsif ($_ eq 'set_contributor_type_text') {
      # Not sure what's supposed to happen here...
    } elsif ($_ eq 'set_note_type_id') {
      my $name = $params->{name};
      $out = $refdata->{instanceNoteTypes}->{$name} or die "Can't find instanceNoteType for $name";
    } elsif ($_ eq 'set_alternative_title_type_id') {
      my $name = $params->{name};
      $out = $refdata->{alternativeTitleTypes}->{$name} || $refdata->{alternativeTitleTypes}->{'Other title'} or die "Can't find alternativeTitleType for $name";
    } elsif ($_ eq 'set_electronic_access_relations_id') {
      my $ind = $field->indicator(2);
      my $name = $relations->{$ind} || '';
      $out = $refdata->{electronicAccessRelationships}->{$name} || '';
    } elsif ($_ eq 'set_classification_type_id') {
      my $name = $params->{name};
      $out = $refdata->{classificationTypes}->{$name} or die "Can't find classificationType for $name";
    } elsif ($_ eq 'set_instance_format_id') {
      $out = $refdata->{instanceFormats}->{$out} || '';
    } elsif ($_ eq 'set_publisher_role') {
      my $ind2 = $field->indicator(2);
      $out = $pub_roles->{$ind2} || '';
    } elsif ($_ eq 'capitalize') {
      $out = ucfirst $out;
    } elsif ($_ eq 'char_select') {
      my $from = $params->{from};
      my $to = $params->{to};
      my $len = $to - $from;
      if (length($out) > $from) {
        $out = substr($out, $from, $len);
      }
    } elsif ($_ eq 'set_instance_type_id') {
      if ($field->tag() gt '009') {
        my $code = $field->subfield('b');
        $out = $refdata->{instanceTypes}->{$code};
      } else {
        $out = '';
      }
    } elsif ($_ eq 'set_issuance_mode_id') {
      $out = '';
    } elsif ($_ eq 'set_identifier_type_id_by_value') {
      my $name;
      my $data = $field->subfield('a');
      if ($data && $data =~ /^(\(OCoLC\)|ocm|ocn|on).*/) {
        $name = 'OCLC';
      } else {
        $name = 'System control number';
      }
      $out = $refdata->{identifierTypes}->{$name} or die "Can't find identifierType for $name";
    } elsif ($_ eq 'remove_substring') {
      my $ss = $params->{substring};
      $out =~ s/$ss//g;
    } elsif ($_ eq 'set_note_staff_only_via_indicator') {
      if ($field->indicator(1) eq '0') {
        $out = 'true';
      } else {
        $out = 'false';
      }
    }
  }
  return $out;
}

my $mapping_rules = getRules($rules_file);

my $ftypes = {
  id => 'string',
  hrid => 'string',
  source => 'string',
  title => 'string',
  indexTitle => 'string',
  alternativeTitles => 'array.object',
  editions => 'array',
  series => 'array.object',
  identifiers => 'array.object',
  contributors => 'array.object',
  subjects => 'array.object',
  classifications => 'array.object',
  publication => 'array.object',
  publicationFrequency => 'array',
  publicationRange => 'array',
  electronicAccess => 'array.object',
  instanceTypeId => 'string',
  instanceFormatIds => 'array',
  physicalDescriptions => 'array',
  languages => 'array',
  notes => 'array.object',
  modeOfIssuanceId => 'string',
  catalogedDate => 'string',
  previouslyHeld => 'boolean',
  staffSuppress => 'boolean',
  discoverySuppress => 'boolean',
  statisticalCodeIds => 'array',
  sourceRecordFormat => 'string',
  statusId => 'string',
  statusUpdatedDate => 'string',
  tags => 'object',
  holdingsRecords2 => 'array.object',
  natureOfContentTermIds => 'array.string'
};

# We need to know upfront which tags support repeated subfields or require preprocessing (think 880s).
my $field_replace = {};
my $repeat_subs = {};
foreach (keys %{ $mapping_rules }) {
  my $rtag = $_;
  foreach (@{ $mapping_rules->{$rtag} }) {
    if ($_->{entityPerRepeatedSubfield}) {
      my $conf = $_;
      foreach (@{ $conf->{entity} }) {
        push @{ $repeat_subs->{$rtag} }, $_->{subfield}->[0] if $_->{target} !~ /Id$/;
      }
    }
    if ($_->{fieldReplacementBy3Digits}) {
      my $frules = {};
      foreach (@{ $_->{fieldReplacementRule} }) {
        $frules->{$_->{sourceDigits}} = $_->{targetField};
      }
      $_->{frules} = $frules;
      $field_replace->{$rtag} = $_;
      delete $mapping_rules->{$rtag};
    }
  }
}

my $resp = {
  instances => [],
  srs => [],
  snapshots => [],
  relationships => [],
  pst => [],
  stats => { instances=>0, srs=>0, snapshots=>0, relationships=>0, pst=>0, errors=>0 }
};

foreach (@ARGV) {
  my $infile = $_;
  if (! -e $infile) {
    die "Can't find raw Marc file!"
  } 
  

  my $dir = dirname($infile);
  my $fn = basename($infile, '.mrc', '.marc', '.out');

  my $snapshot = make_snapshot();
  my $snapshot_id = $snapshot->{jobExecutionId};
  push @{ $resp->{snapshots} }, $snapshot;
  $resp->{stats}->{snapshots}++;
  
  # open a collection of raw marc records
  $/ = "\x1D";
 
  open RAW, "<:encoding(UTF-8)", $infile;
  my $inst_recs;
  my $srs_recs;
  my $hrecs;
  my $irecs;
  my $count = 0;
  my $hrids = {};
  my $rec;
  while (<RAW>) {
    $count++;
    my $raw = $_;
    $rec = {
      id => '',
      alternativeTitles => [],
      editions => [],
      series => [],
      identifiers => [],
      contributors => [],
      subjects => [],
      classifications => [],
      publication => [],
      publicationFrequency => [],
      publicationRange => [],
      electronicAccess => [],
      instanceFormatIds => [],
      physicalDescriptions => [],
      languages => [],
      notes => [],
      staffSuppress => JSON::false,
      discoverySuppress => JSON::false,
      statisticalCodeIds => [],
      tags => {},
      holdingsRecords2 => [],
      natureOfContentTermIds => [],
      statusId => '52a2ff34-2a12-420d-8539-21aa8d3cf5d8',
      source => 'MARC',
      instanceTypeId => ''
    };
    my $relid = '';
    my $marc = eval {
      MARC::Record->new_from_usmarc($raw);
    };
    next unless $marc;

    $hrid_conf->{inst}->{cur}++;
    my $cur_hrid = $hrid_conf->{inst}->{cur};
    my $prefix = $hrid_conf->{inst}->{pre};
    my $ctrlnum = sprintf("$prefix%011d", $cur_hrid);

    my $f001 = MARC::Field->new('001', $ctrlnum);
    $marc->insert_fields_ordered($f001);

    my $srsmarc = $marc;
    if ($marc->field('880')) {
      $srsmarc = $marc->clone();
    }
    my $ldr = $marc->leader();
    my $blevel = substr($ldr, 7, 1);
    my $type = substr($ldr, 6, 1);
    my $inst_type = $rtypes->{$type} || 'zzz';
    $rec->{instanceTypeId} = $refdata->{instanceTypes}->{$inst_type};
    my $mode_name = $blvl->{$blevel} || 'Other';

    my $lc_mode_name = lc $mode_name;
    if ($lc_mode_name eq 'monograph') { $lc_mode_name = 'single unit'}
    if ($refdata->{issuanceModes}->{$mode_name}) {
      $rec->{modeOfIssuanceId} = $refdata->{issuanceModes}->{$mode_name};
    } elsif ($refdata->{issuanceModes}->{$lc_mode_name}) {
      $rec->{modeOfIssuanceId} = $refdata->{issuanceModes}->{$lc_mode_name};
    } else {
      $rec->{modeOfIssuanceId} = $refdata->{issuanceModes}->{unspecified};
    }
    my @marc_fields = $marc->fields();
    MARC_FIELD: foreach my $field (@marc_fields) {
      my $tag = $field->tag();
      my $fr = $field_replace->{$tag} || '';
      if ($fr) {
        my $sf = $fr->{subfield}[0];
        my $sdata = $field->subfield($sf) || next;
        $sdata =~ s/^(\d{3}).*/$1/;
        my $rtag = $fr->{frules}->{$sdata} || $sdata;
        if ($rtag ne '880' && $rtag =~ /^\d\d\d$/) {
          $field->set_tag($rtag);
          push @marc_fields, $field;
        }
        next;
      }
      if (($tag =~ /^(70|71|1)/ && !$field->subfield('a')) || ($tag == '856' && !$field->subfield('u'))) {
        next;
      }
      
      # Let's determine if a subfield is repeatable, if so append separate marc fields for each subfield;
      foreach (@{ $repeat_subs->{$tag} }) {
        my $main_code = $_;
        my $all_codes = join '', @{ $repeat_subs->{$tag} };
        my @sf = $field->subfield($main_code);
        my $occurence = @sf;
        if ($occurence > 0 && !$field->{_seen}) {
          my $new_field = {};
          my $i = 0;
          my @subs = $field->subfields();
          foreach (@subs) {
            my ($code, $sdata) = @$_;
            $new_field = MARC::Field->new($tag, $field->{_ind1}, $field->{_ind2}, $code => $sdata);
            $new_field->{_seen} = 1;
            $i++;
            my @ncode = ('');
            if ($subs[$i]) {
              @ncode = @{ $subs[$i] };
            }
            if ((index($all_codes, $ncode[0]) != -1 && $new_field->{_tag}) || !$ncode[0]) {
              push @marc_fields, $new_field;
            }
          }
          next MARC_FIELD;
        } 
      }
      
      my $fld_conf = $mapping_rules->{$tag};
      my @entities;
      if ($fld_conf) {
        if ($fld_conf->[0]->{entity}) {
          foreach (@{ $fld_conf }) {
            if ($tag eq '024') {
              if ($_->{indicators}) {
                my $ind1 = $_->{indicators}->{ind1};
                $ind1 =~ s/\*//;
                next if $ind1 && $field->indicator(1) ne $ind1;
              } elsif ($field->indicator(1) =~ /[12]/) {
                next;
              }
            }
            if ($_->{entity}) {
              push @entities, $_->{entity};
            }
          }
        } else {
          @entities = $fld_conf;
        }
        foreach (@entities) {
          my @entity = @$_;
          my $data_obj = {};
          foreach (@entity) {
            if ($_->{alternativeMapping}) {
              push @entity, $_->{alternativeMapping};
            }
            if ($_->{target} =~ /precedingTitle|succeedingTitle/) {
              next;
            }
            my @required;
            if ( $_->{requiredSubfield} ) {
              @required = @{ $_->{requiredSubfield} };
            }
            if ($required[0] && !$field->subfield($required[0])) {
              next;
            }
            my @targ;
            my $flavor;
            if ($_->{target}) {
              @targ = split /\./, $_->{target};
              $flavor = $ftypes->{$targ[0]};
            }
            my $data = process_entity($field, $_);
            next unless $data;
            if ($flavor eq 'array') {
              if ($_->{subFieldSplit}) { # subFieldSplit is only used for one field, 041, which may have a lang string like engfreger.
                my $val = $_->{subFieldSplit}->{value};
                my @splitdata = $data =~ /(\w{$val})/g;
                push @{ $rec->{$targ[0]} }, @splitdata;
              } else {
                push @{ $rec->{$targ[0]} }, $data;
              }
            } elsif ($flavor eq 'array.object') {
              $data_obj->{$targ[0]}->{$targ[1]} = $data;
            } elsif ($flavor eq 'object') {
            } elsif ($flavor eq 'boolean') {
            } else {
              $rec->{$targ[0]} = $data;
            }
          }
          foreach (keys %$data_obj) {
            if ($ftypes->{$_} eq 'array.object') {
              push @{ $rec->{$_} }, $data_obj->{$_};
            }
          }
        }
      }
      if ($tag eq '787') {
        $relid = $field->subfield('w') || '';
      }
    }
    # Do some some record checking and cleaning
    $rec->{subjects} = dedupe(@{ $rec->{subjects} });
    $rec->{languages} = dedupe(@{ $rec->{languages} });
    $rec->{series} = dedupe(@{ $rec->{series} });
    if ($marc->field('008')) {
      my $cd = $marc->field('008')->data();
      my $yr = substr($cd, 0, 2);
      my $mo = substr($cd, 2, 2);
      my $dy = substr($cd, 4, 2);
      if ($yr =~ /^[012]/) {
        $yr = "20$yr";
      } else {
        $yr = "19$yr";
      }
      $rec->{catalogedDate} = "$yr-$mo-$dy";
    }
    
    # delete duplicate contributor types.
    foreach (@{ $rec->{contributors} }) {
      if ($_->{contributorTypeId} && $_->{contributorTypeText}) {
        delete $_->{contributorTypeText};
      }
    }
    
    # Assign uuid based on hrid;
    if (!$rec->{hrid}) {
      die "No HRID found in record $count";
    }
    my $hrid = $rec->{hrid};
    if (!$hrids->{$hrid} && $marc->title()) {
      
      if ($relid) {
        my $superid = uuid($relid);
        my $rtype = $refdata->{instanceRelationshipTypes}->{'bound-with'};
        my $relobj = { superInstanceId=>$superid, subInstanceId=>$rec->{id}, instanceRelationshipTypeId=>$rtype };
        push @{ $resp->{relationships} }, $relobj;
        $resp->{stats}->{relationships}++;
      }
      push @{ $resp->{instances} }, $rec;
      my $srs = make_srs($srsmarc, $raw, $rec->{id}, $rec->{hrid}, $snapshot_id);
      push @{ $resp->{srs} }, $srs;
      $hrids->{$hrid} = 1;
      $resp->{stats}->{instances}++;
      $resp->{stats}->{srs}++;

      # make preceding succeding titles
      foreach my $f ($marc->field('78[05]')) {
        my $presuc = {};
        my $pstype = 1;
        $presuc->{title} = $f->as_string('ast');
        if ($f->tag() eq '785') {
          $presuc->{precedingInstanceId} = $rec->{id};
        } else {
          $presuc->{succeedingInstanceId} = $rec->{id};
          $pstype = 2;
        }
        foreach my $sf (('w', 'x')) {
          my $idtype = $refdata->{identifierTypes}->{'Other standard identifier'};
          foreach ($f->subfield($sf)) {
            if ($sf eq 'Z') {
              my $instid = uuid($_);
              if ($pstype == 1) {
                $presuc->{succeedingInstanceId} = $instid;
              } else {
                $presuc->{precedingInstanceId} = $instid;
              }
            } 
            if (/OCoLC|ocm|ocn/) {
              $idtype = $refdata->{identifierTypes}->{'OCLC'};
            } elsif (/DLC/) {
              $idtype = $refdata->{identifierTypes}->{'LCCN'};
            } elsif (/^\d{4}-[0-9Xx]{4}/) {
              $idtype = $refdata->{identifierTypes}->{'ISSN'};
            } elsif (/^[0-9Xx]{10,13}/) {
              $idtype = $refdata->{identifierTypes}->{'ISBN'};
            }
            my $idObj = { value=>$_, identifierTypeId=>$idtype };
            push @{ $presuc->{identifiers} }, $idObj;
          }
        }
        push @{ $resp->{pst} }, $presuc;
        $resp->{stats}->{pst}++;
      } 
    } else {
      if ($hrids->{$hrid}) {
        print "ERROR Duplicate HRID: $hrid\n";
      } 
      if (!$rec->{title}) {
        print "ERROR $hrid has no title\n"
      }
      $resp->{stats}->{errors}++;
    }
  }

  my $tt = time() - $start;
  $resp->{stats}->{total} = $count;
  $resp->{stats}->{timeSeconds} = $tt;
  print $json->pretty->encode($resp);
}

sub dedupe {
  my @out;
  my $found = {};
  foreach (@_) { 
    $found->{$_}++;
    if ($found->{$_} < 2) {
      push @out, $_;
    }
  }
  return [ @out ];
}

sub make_snapshot {
  my $snap_path = shift;
  my @t = localtime();
  my $dt = sprintf("%4s-%02d-%02d", $t[5] + 1900, $t[4] + 1, $t[3]);
  my $snap_id = uuid($dt);
  my $snap = {
    jobExecutionId=>$snap_id,
    status=>"COMMITTED",
    processingStartedDate=>"${dt}T00:00:00"
  };
  return $snap;
}

sub make_srs {
    my $marc = shift;
    my $raw = shift;
    my $iid = shift;
    my $hrid = shift;
    my $snap_id = shift;
    my $hid = shift || '';
    my $srs = {};
    if ($hid && $marc->field('852')) {
      my $field = $marc->field('852');
      if ($field->subfield('b')) {
        my $loc = $field->subfield('b');
        $loc = "LANE-$loc";
        $field->update('b' => $loc);
      }
    }

    my $mij = MARC::Record::MiJ->to_mij($marc);
    my $parsed = decode_json($mij);
    
    $srs->{id} = uuid($iid . 'srs');
    my $nine = {};
    $nine->{'999'} = { subfields=>[ { 'i'=>$iid || $hid }, { 's'=>$srs->{id} } ] };
    $nine->{'999'}->{'ind1'} = 'f';
    $nine->{'999'}->{'ind2'} = 'f';
    push @{ $parsed->{fields} }, $nine;
    $srs->{snapshotId} = $snap_id;
    $srs->{matchedId} = $srs->{id};
    $srs->{generation} = 0;
    $srs->{rawRecord} = { id=>$srs->{id}, content=>$raw };
    $srs->{parsedRecord} = { id=>$srs->{id}, content=>$parsed };
    if ($hid) {
      $srs->{externalIdsHolder} = { holdingsId=>$hid, holdingsHrid=>$hrid };
      $srs->{recordType} = 'MARC_HOLDING';
    }
    else {
      $srs->{externalIdsHolder} = { instanceId=>$iid, instanceHrid=>$hrid };
      $srs->{recordType} = 'MARC_BIB';
    }
    return $srs;
}
