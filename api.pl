#!/usr/bin/perl

use JSON;
use strict;
use warnings;

my $json = JSON->new;
$json->canonical(1);
$json->pretty(1);

{
  package OUF::API::Server;
  
  use HTTP::Server::Simple::CGI;
  use base qw(HTTP::Server::Simple::CGI);
  use Data::Dumper;
  use open qw( :std :encoding(UTF-8) );

  if (! -e 'stash') {
    mkdir 'stash';
  }
  require './mapper.pl';
  my $fcount = 0;

  my %dispatch = (
    '/' => \&resp_menu,
    '/marc2inst' => \&marc2inst
  );

  sub handle_request {
    my $self = shift;
    my $cgi  = shift;
   
    my $path = $cgi->path_info();
    my $handler = $dispatch{$path};
 
    if (ref($handler) eq "CODE") {
        print "HTTP/1.0 200 OK\r\n";
        print $cgi->header( -type => 'application/json; charset=utf-8');
        $handler->($cgi);
    } else {
        print "HTTP/1.0 404 Not found\r\n";
        print $cgi->header,
              $cgi->start_html('Not found'),
              $cgi->h1('Not found'),
              $cgi->end_html;
    }
  }

  sub marc2inst {
    my $cgi  = shift;
    return if !ref $cgi;
    my $method = $cgi->request_method();
    if ($method ne 'POST') {
      &not_allowed($cgi);
      return;
    }
    $fcount++;
    my $fn = "stash/$fcount.mrc";
    open STSH, ">", $fn or print "WARN Can't open stash file at $fn";
    print STSH '' . $cgi->param('POSTDATA');
    close STSH;
    mapper($fn);
    return;
  }

  sub resp_menu {
    my $cgi  = shift;
    return if !ref $cgi;
    
    my $out = { endpoints => [] };

    my @ends = qw(marc2inst);
    foreach (@ends) {
      push @{ $out->{endpoints} }, $_;
    }
    print $json->encode($out);
  }

  sub not_allowed {
    my $cgi = shift;
    print '{ "error": { "status": "405", "message": "Method Not Allowed" } }';
    return;
  }

}

$SIG{TERM} = sub { 
  while (<stash/*>) {
    unlink $_;
  }
  die "See ya later...\n";
 };
my $port = '8888';
my $pid = OUF::API::Server->new($port)->background();
