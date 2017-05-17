#!/usr/bin/perl -w

use strict;

use lib '.';
use camel;

my @lines = ();
my %deprecated = ();

sub adjust {
    my ($i, $n, %h) = @_;

    foreach my $k (keys %h) {
	my @vals = split(/,/, $h{$k});
	my $nvals = "";
	foreach my $v (@vals) {
	    if ($v > $i) {
		$v += $n;
	    }
	    $nvals .= "$v";
	}
	$h{$k} = $nvals;
    }

    return %h;
}

# Return a line containing the entire signature of the method.
# This signature may have been spread across many lines, this
# function will gobble up those lines and shrink the @lines
# @ array as required.
sub method_decl {
    my ($i, $is_interface) = @_;
    my @newline = ("");
    my $balanced = 0;
    my $count = 0;

    chomp($lines[$i]);
    @newline = ("$lines[$i]");

    while (($balanced == 0) && ($lines[$i] =~ /{/ eq "") && (!$is_interface)) {
	my ($op) = split(/{/, $lines[$i]); $op =~ s/[^(]//g;
	my ($cp) = split(/{/, $lines[$i]); $cp =~ s/[^)]//g;
	$balanced = 1 if (length($op) == length($cp));
	chomp($lines[$i + 1]);
	@newline = ("$newline[0]$lines[$i + 1]");
	splice(@lines, $i, 2, @newline);
	$count += 1; # for every line we take out, we must put one back
    }
    $lines[$i] = "$lines[$i]\n";
    while ($count--) { splice(@lines, ($i + 1), 0, ("\n")); }
    return $lines[$i];
}

# Return the name of the method.
sub method_name {
    my ($line) = @_;
    my $methodname = "";

    ($line) = split(/{/, $line);
    if (defined($line)) {
	($methodname) = reverse(split(/ /, (split(/\(/, $line))[0]));
	chomp($methodname);
    }
    return $methodname;
}

# Process lines within $classname, return a hash.  Keys are method
# names, values are comma delimited line numbers where they occur.
sub public_methods_from {
    my ($i, $classname, $type) = @_;
    my %results;
    my $bc = 1;
    my $bl = "";
    my $dep = 0;

    while ($bc > 0 && $i <= $#lines) {
	my $line = $lines[$i];
	if ($line =~ /\@deprecated/) { $dep = 1; }
	# look only for public method declarations
	if ($line =~ /^\s*public/ && $line =~ /\(/) {
	    $line = method_decl($i, ($type eq "interface" ? 1 : 0));
	    my $methodname = method_name($line);
	    if ($methodname ne $classname) {
		if (! defined($results{$methodname})) {
		    $results{$methodname} = "$i";
		} else {
		    $results{$methodname} = "$results{$methodname},$i";
		}
		if ($dep == 1) {
		    $deprecated{$methodname} = $line;
		    $dep = 0;
		}
	    }
	}
	# XXX this assumes that there are no unbalanced braces in comments
	$bl = $lines[$i]; $bl =~ s/[^{]//g; $bc += length($bl);
	$bl = $lines[$i]; $bl =~ s/[^}]//g; $bc -= length($bl);
	$i++;
    }
    foreach my $key (keys %deprecated) {
	$deprecated{$key} = method_type_sig($deprecated{$key});
#	print "key: $key\t$deprecated{$key}\n";
    }
    return %results;
}

##
sub method_type_sig {
    my ($line) = @_;
    my $typesig = "";

    my ($sig, $tail) = split(/{/, $line);
    $typesig = $sig; $typesig =~ s/.*public ([^ \t]+).*/$1/;
    $typesig = "$typesig|";
    (my $argument) = $sig =~ /\(([^)]+)\)/;
    if (defined $argument) {
	my @arguments = split(/,/, $argument);
	foreach my $ma (@arguments) {
	    $ma =~ /\s*(\S+)\s+(\S+)/;
	    $typesig .= "$1#";
	}
    }
    return $typesig;
}


##
sub deprecate_method {
    my ($signature_line, $is_interface) = @_;

    my ($sig, $tail) = split(/{/, $signature_line);
    my $method = method_name($sig);
    my $new_method = $camel::under2camel{$method};
    my @arg_names = (); my @arg_types = (); my @arguments = ();

    (my $argument) = $sig =~ /\(([^)]+)\)/;
    if (defined $argument) {
	@arguments = split(/,/, $argument);
	foreach my $ma (@arguments) {
	    $ma =~ /\s*(\S+)\s+(\S+)/;
	    @arg_types = (@arg_types, $1);
	    @arg_names = (@arg_names, $2);
	}
    }
    # When it comes to interfaces, only the new method will be invoked
    # by our code, so leaving the old signature around will confuse
    # programmers.  Worse yet, by leaving around the old signature the
    # javac compiler will insist that the method be defined or that the
    # class be declared abstract, a bug either way you look at it on
    # our part.  The only thing to do is leave it out and go with
    # the new signature only.
    my $result = "";
    if (! $is_interface) {
	$result .= "\t/**\n";
	$result .= "\t *\@deprecated As of Berkeley DB 4.2, replaced by ";
	$result .= "{\@link #$new_method(";
	$result .= join(",", @arg_types);
	$result .= ")}\n";
	$result .= "\t */\n";
	$result .= "\t$sig";
	$result .= "{\n" unless $is_interface;
	if ($signature_line =~ /public void/) {
	    $result .= "\t\t";
	} else {
	    $result .= "\t\treturn ";
	}
	$result .= "$new_method(";
	$result .= join(", ", @arg_names);
	$result .= ");\n";
	$result .= "\t}\n\n";
    }
    $result .= "\n";
    my $new_sig = $sig;
    $new_sig =~ s/$method\(/$new_method\(/;
    $result .= "$new_sig";
    $result .= "{" if ($signature_line =~ /{/);
    $result .= $tail if (defined $tail);
    return $result;
}

sub usage {
    print "camelize.pl [-d] [-m] [-c] <.java files>\n";
    print "      -d  --deprecation\n";
    print "      -m  --methodcalls\n";
    print "      -c  --comments\n";
    print "      -h  \n";
    print "\n";
    print "Update various aspects of Java files according to\n";
    print "the mapping information provided by ./camel.pm file.\n";
}

##
## MAIN

my %public_methods;
my $do_deprecation = 0;
my $do_methodcalls = 0;
my $do_comments = 0;
my @files = ();

foreach my $arg (@ARGV) {
    if ($arg eq "-d" || $arg eq "--deprecation") {
	$do_deprecation = 1;
    } elsif ($arg eq "-m" || $arg eq "--methodcalls") {
	$do_methodcalls = 1;
    } elsif ($arg eq "-c" || $arg eq "--comments") {
	$do_comments = 1;
    } elsif ($arg eq "-h" || $arg eq "--help" || $arg eq "-?") {
	usage();
	exit 0;
    } else {
	push(@files, $arg);
    }
}

if ($do_deprecation + $do_methodcalls + $do_comments == 0) {
    usage();
    exit 0;
}

foreach my $file (@files) {
    open(IN, $file) or die;

    # Read entire file into this array.
    @lines = <IN>;

    my $i = 0;

    # SWIG puts some garbage comments into the code,
    # take them out.
    while (my $line = $lines[$i]) {
	$line =~ s!/\* no exception \*/!!;
	$line =~ s!/\*u_int32_t\*/!!;
	$line =~ s!/\* package \*/!!;
	$lines[$i] = $line;
	$i++;
    }

    # Deprecate all older API methods and add new methods in their place.
    if ($do_deprecation == 1) {
    $i = 0;
    while (my $line = $lines[$i]) {
	if ($line =~ /^\s*public( final)? (class|interface) (\w*)/) {
	    %public_methods = public_methods_from($i, $3, $2);
            my $is_interface = ("$2" eq "interface") ? 1 : 0;
	    foreach my $key (keys %public_methods) {
		# if this is a method that needs to be translated and
		# its translation is not the same name, proceed...
		if (defined($camel::under2camel{$key}) &&
		    ($key ne $camel::under2camel{$key})) {
		    # each method name may appear more than once
		    foreach my $mloc (split(/,/, $public_methods{$key})) {
			# make sure that a method with the translated name
			# and signature of this one doesn't already exist
			my $tm = method_type_sig($lines[$mloc]);
			if ((! (defined $deprecated{$key})) ||
			      ($deprecated{$key} ne $tm)) {
			    my $dep = deprecate_method($lines[$mloc],
			        $is_interface);
			    splice(@lines, $mloc, 1, ($dep));
			}
		    }
		}
	    }
	}
	$i++;
    }}

    # Look for usage of old API, convert to new API.
    if ($do_methodcalls == 1) {
    $i = 0;
    while (my $line = $lines[$i]) {
	foreach my $key (keys %camel::under2camel) {
	    if ( ($line =~ /$key\(/) &&
		!($line =~ /public/)) { # make sure not to rename the
                                        # method decl lines as they may
                                        # may be the deprecated cover method
		$line =~ s/$key\(/$camel::under2camel{$key}\(/g;
		$lines[$i] = $line;
	    }
	}
	$i++;
    }}

    # Look for comments referencing old API, convert to new API.
    if ($do_comments == 1) {
    $i = 0;
    my $in_comment = 0; my $comment = ""; my $rest = "";
    while (my $line = $lines[$i]) {
	if ($in_comment == 0) {
	    ($rest, $comment) = split(/\/\*/, $line);
	    if (defined $comment) {
		$in_comment = 1;
		$i--;
	    }
	} else {
	    # NOTE: if I were really picky I would just modify the
	    # comment portion and rebuild the line from that for the
	    # cases where a line opens with code then a '/*' comment.
	    ($comment, $rest) = split(/\*\//, $line);
	    foreach my $key (keys %camel::under2camel) {
		if ($line =~ /[^a-zA-Z]$key([^a-zA-Z]|\n)/) {
		    $line =~ s/$key/$camel::under2camel{$key}/g;
		    $lines[$i] = $line;
		}
	    }
	    if (defined $rest) {
		$in_comment = 0;
	    }
	}
	if ($line =~ /\/\//) {
	    ($rest, $comment) = split(/\/\//, $line);
	    foreach my $key (keys %camel::under2camel) {
		$comment =~ s/\(\w\)$key\(\w\)/$1$camel::under2camel{$key}$2/g;
	    }
	    $lines[$i] = "$rest//$comment";
	}
	$i++;
    }}

    print @lines;
}
