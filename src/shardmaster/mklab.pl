#!/usr/bin/perl -w

#
# ./mklab output-directory files...
#
# strips out lines from
#   // SOL
# to
#   // END
#
# eliminates DELETE
# e.g. func DELETELock(...)
#

use strict;
use File::Copy qw(copy);

sub usage {
    print STDERR "Usage: mklab.pl output-directory files...\n";
    exit(1);
}

sub onefile {
    my($infile, $outfile) = @_;

    if ($infile !~ /\.go$/){
        print STDERR "copying non-Go $infile without SOL parsing\n";
        die "cannot copy $infile > $outfile" if !copy($infile, $outfile);
        return;
    }

    die "cannot read $infile" if !open(IF, $infile);
    my $out = "";

    my $omit = 0;
    while(<IF>){
        s/DELETE//g;
        if(/ SOL$/){
            if($omit == 0){
                $omit = 1;
            } else {
                die "nested SOL";
            }
        } elsif(/SOL/){
            die "malformed SOL";
        } elsif(/ END$/){
            if($omit == 1){
                $omit = 0;
            } else {
                die "extra END in $infile";
            }
        } elsif(/END/){
            die "malformed END";
        } elsif($omit == 0){
            $out .= $_;
        }
    }
    close(IF);

    if($omit){
        die "SOL without END";
    }

    die "cannot write $outfile" if !open(OF, ">$outfile");
    print OF $out;
    close(OF);
}

sub main {
    if($#ARGV < 1){
        usage();
    }
    
    my $dir = $ARGV[0];

    system("mkdir -p $dir");
    if(! -d $dir){
        print STDERR "cannot create directory $dir\n";
        exit(1)
    }

    for(my $i = 1; $i <= $#ARGV; $i++){
        my $f = $ARGV[$i];
        onefile($f, $dir . "/" . $f);
    }
}

main();
exit(0);
