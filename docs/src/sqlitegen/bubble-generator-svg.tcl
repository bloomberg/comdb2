#!/usr/bin/env tclsh
#
# Generate the syntax bubble diagrams as SVG files.  Unlike
# bubble-generator.tcl this does not need Tk (or ImageMagick): it sources
# bubble-generator.tcl under plain tclsh with a small emulation of the Tk
# canvas, so the layout code and the diagram data are shared with the GIF
# generator.  Text is emitted as real <text> elements, so it's searchable.
#
# Usage:
#
#   tclsh bubble-generator-svg.tcl ?-o OUTDIR? ?NAME ...?
#   tclsh bubble-generator-svg.tcl -u PATH ?-u PATH ...? ?NAME ...?
#
# The first form writes NAME.svg files to OUTDIR, which defaults to
# ../../images (same place the GIFs go).
#
# The second form updates diagrams inlined in Markdown files.  PATH is a
# Markdown file, or a directory to search for them.  Every inlined <svg>
# whose <title> names a diagram is replaced by a freshly generated copy,
# keeping its role and aria-label attributes.  To add a diagram to a page,
# put a placeholder where it should go and run this, e.g.:
#
#   <svg role="img" aria-label="BEGIN">
#   <title>begin-stmt</title>
#   </svg>
#
# With no NAMEs, every diagram in bubble-generator-data.tcl is generated
# or updated.
#

set dir [file dirname [file normalize [info script]]]
set outdir [file join $dir .. .. images]
set names {}
set update_paths {}
for {set i 0} {$i<[llength $argv]} {incr i} {
  set a [lindex $argv $i]
  switch -- $a {
    -o      {set outdir [lindex $argv [incr i]]}
    -u      {lappend update_paths [lindex $argv [incr i]]}
    -h - -help - --help {
      set me [file tail [info script]]
      puts "usage: $me ?-o OUTDIR? ?NAME ...?"
      puts "       $me -u PATH ?-u PATH ...? ?NAME ...?"
      exit 0
    }
    default {lappend names $a}
  }
}

# Stand-ins for the Tk commands used at the top level of bubble-generator.tcl.
#
foreach cmd {toplevel canvas frame button pack wm update} {
  proc $cmd args {}
}

# Advance widths for characters 32..126, in 1/1000 em.
#
set char_widths(Helvetica-Bold) {
  278 333 474 556 556 889 722 278 333 333 389 584 278 333 278 278 556 556 556
  556 556 556 556 556 556 556 333 333 584 584 584 611 975 722 722 722 722 667
  611 778 722 278 556 722 611 833 722 778 667 778 722 667 611 722 667 944 667
  667 611 333 278 333 584 556 278 556 611 556 611 556 333 611 611 278 278 556
  278 889 611 611 611 611 389 556 333 611 556 778 556 556 500 389 280 389 584
}
set char_widths(DejaVuSans-Bold) {
  348 456 521 838 696 1002 872 306 457 457 523 838 380 415 380 365 696 696 696
  696 696 696 696 696 696 696 400 400 838 838 838 580 1000 774 762 734 830 683
  683 821 837 372 372 775 637 995 837 850 733 850 770 720 682 812 774 1103 771
  724 725 457 365 457 838 500 500 675 716 593 716 678 435 716 712 343 343 665
  343 1042 712 687 716 716 493 595 478 712 652 924 645 652 582 712 365 712 838
}

# The diagrams were laid out using the metrics of the screen fonts on the
# (Linux) machine that produced the GIFs, while the text itself was drawn
# by the postscript output in Helvetica-Bold at the nominal font size in
# canvas units.  These are the screen metrics that reproduce the GIF
# layouts:  Helvetica rendered as a Helvetica-metric font, and GillSans,
# which wasn't installed, fell back to DejaVu Sans.  "em" is the pixel
# size, "height" the font's line height.
#
array set screen_font {
  {Helvetica 16 bold}  {font Helvetica-Bold em 21.4 height 28}
  {GillSans 14 bold}   {font DejaVuSans-Bold em 19.6 height 22}
}

# Output scale.  The GIFs are the canvas postscript (72/96 points per
# canvas pixel) rasterized at 80 DPI.
#
set SCALE [expr {0.75*80/72.0}]

namespace eval cv {
  variable nextid 0
  variable order {}
  variable type
  variable coords
  variable opts
  variable tags
}

# Width of $txt in $font, in 1/1000 em.
#
proc string_width {font txt} {
  set w 0
  foreach ch [split $txt {}] {
    scan $ch %c c
    if {$c<32 || $c>126} {set c 63}
    incr w [lindex $::char_widths($font) [expr {$c-32}]]
  }
  return $w
}

proc cv::text_size {font txt} {
  set m $::screen_font($font)
  set w [string_width [dict get $m font] $txt]
  return [list [expr {round($w*[dict get $m em]/1000.0)}] [dict get $m height]]
}

# Round the way Tk's TkIncludePoint does: (int)(x+0.5), truncating to zero.
#
proc cv::rnd {x} {return [expr {int($x+0.5)}]}

proc cv::create {t args} {
  variable nextid; variable order
  variable type; variable coords; variable opts; variable tags
  set c {}
  while {[llength $args] && ![regexp {^-[a-z]} [lindex $args 0]]} {
    lappend c [lindex $args 0]
    set args [lrange $args 1 end]
  }
  if {$t!="line" && $t!="text"} {
    # Tk puts the corners of rectangles, ovals and arcs in order.
    foreach {x0 y0 x1 y1} $c break
    if {$x0>$x1} {set t0 $x0; set x0 $x1; set x1 $t0}
    if {$y0>$y1} {set t0 $y0; set y0 $y1; set y1 $t0}
    set c [list $x0 $y0 $x1 $y1]
  }
  set id [incr nextid]
  set type($id) $t
  set coords($id) $c
  set o [dict create -width 1 -arrow none -start 0 -extent 90]
  foreach {k v} $args {dict set o $k $v}
  set tags($id) [expr {[dict exists $o -tags] ? [dict get $o -tags] : {}}]
  dict unset o -tags
  set opts($id) $o
  lappend order $id
  return $id
}

proc cv::find {tagOrId} {
  variable order; variable tags
  if {[string is integer -strict $tagOrId]} {
    return [expr {[lsearch -exact $order $tagOrId]>=0 ? $tagOrId : {}}]
  }
  if {$tagOrId=="all"} {return $order}
  set r {}
  foreach id $order {
    if {[lsearch -exact $tags($id) $tagOrId]>=0} {lappend r $id}
  }
  return $r
}

proc cv::move {tagOrId dx dy} {
  variable coords
  foreach id [find $tagOrId] {
    set c {}
    foreach {x y} $coords($id) {
      lappend c [expr {$x+$dx}] [expr {$y+$dy}]
    }
    set coords($id) $c
  }
}

proc cv::addtag {newtag how tagOrId} {
  variable tags
  foreach id [find $tagOrId] {
    if {[lsearch -exact $tags($id) $newtag]<0} {lappend tags($id) $newtag}
  }
}

proc cv::dtag {tagOrId {deltag {}}} {
  variable tags
  if {$deltag==""} {set deltag $tagOrId}
  foreach id [find $tagOrId] {
    set i [lsearch -exact $tags($id) $deltag]
    if {$i>=0} {set tags($id) [lreplace $tags($id) $i $i]}
  }
}

proc cv::delete {tagOrId} {
  variable order
  variable type; variable coords; variable opts; variable tags
  foreach id [find $tagOrId] {
    set i [lsearch -exact $order $id]
    set order [lreplace $order $i $i]
    unset type($id) coords($id) opts($id) tags($id)
  }
}

proc cv::lower args {}
proc cv::config args {}

# Return the coordinates of a line (shortened where it meets an arrowhead,
# as Tk does) followed by a list of arrowhead polygons.
#
proc cv::line_geometry {id} {
  variable coords; variable opts
  set c $coords($id)
  set width [dict get $opts($id) -width]
  set arrow [dict get $opts($id) -arrow]
  set shapeA [expr {8+0.001}]
  set shapeB [expr {10+0.001}]
  set shapeC [expr {3+$width/2.0+0.001}]
  set frac [expr {($width/2.0)/$shapeC}]
  set backup [expr {$frac*$shapeB + $shapeA*(1.0-$frac)/2.0}]
  set polys {}
  set n [llength $c]
  foreach {end tip from} [list first 0 2 last [expr {$n-2}] [expr {$n-4}]] {
    if {$arrow!=$end && $arrow!="both"} continue
    set px [lindex $c $tip]
    set py [lindex $c [expr {$tip+1}]]
    set dx [expr {$px-[lindex $c $from]}]
    set dy [expr {$py-[lindex $c [expr {$from+1}]]}]
    set len [expr {hypot($dx,$dy)}]
    if {$len==0} {
      set sin 0.0; set cos 0.0
    } else {
      set sin [expr {$dy/$len}]; set cos [expr {$dx/$len}]
    }
    set vx [expr {$px-$shapeA*$cos}]
    set vy [expr {$py-$shapeA*$sin}]
    set x1 [expr {$px-$shapeB*$cos+$shapeC*$sin}]
    set y1 [expr {$py-$shapeB*$sin-$shapeC*$cos}]
    set x4 [expr {$x1-2*$shapeC*$sin}]
    set y4 [expr {$y1+2*$shapeC*$cos}]
    lappend polys [list $px $py $x1 $y1 \
        [expr {$x1*$frac+$vx*(1-$frac)}] [expr {$y1*$frac+$vy*(1-$frac)}] \
        [expr {$x4*$frac+$vx*(1-$frac)}] [expr {$y4*$frac+$vy*(1-$frac)}] \
        $x4 $y4]
    lset c $tip [expr {$px-$backup*$cos}]
    lset c [expr {$tip+1}] [expr {$py-$backup*$sin}]
  }
  return [list $c $polys]
}

# Return the endpoints of an arc, and its oval's center and radii.
#
proc cv::arc_geometry {id} {
  variable coords; variable opts
  foreach {x0 y0 x1 y1} $coords($id) break
  set start [dict get $opts($id) -start]
  set extent [dict get $opts($id) -extent]
  set cx [expr {($x0+$x1)/2.0}]
  set cy [expr {($y0+$y1)/2.0}]
  set rx [expr {($x1-$x0)/2.0}]
  set ry [expr {($y1-$y0)/2.0}]
  set pi [expr {acos(-1)}]
  set a0 [expr {$start*$pi/180}]
  set a1 [expr {($start+$extent)*$pi/180}]
  return [list [expr {$cx+$rx*cos($a0)}] [expr {$cy-$ry*sin($a0)}] \
               [expr {$cx+$rx*cos($a1)}] [expr {$cy-$ry*sin($a1)}] \
               $cx $cy $rx $ry]
}

# Text position:  left, top, width, height.
#
proc cv::text_box {id} {
  variable coords; variable opts
  foreach {x y} $coords($id) break
  foreach {w h} [text_size [dict get $opts($id) -font] \
                           [dict get $opts($id) -text]] break
  set left [expr {int(floor($x+0.5))-$w/2}]
  set top [expr {int(floor($y+0.5))-$h/2}]
  return [list $left $top $w $h]
}

# Compute an item's bounding box following the rules of the Tk canvas.
#
proc cv::item_bbox {id} {
  variable type; variable coords; variable opts
  set c $coords($id)
  set width [dict get $opts($id) -width]
  switch $type($id) {
    line {
      set polys [lindex [line_geometry $id] 1]
      set x0 [set x1 [expr {int([lindex $c 0])}]]
      set y0 [set y1 [expr {int([lindex $c 1])}]]
      set pts [lrange $c 2 end]
      set w [expr {int($width+0.5)}]
      if {$w<1} {set w 1}
      foreach {x y} $pts {
        set x [rnd $x]; set y [rnd $y]
        if {$x<$x0} {set x0 $x}; if {$x>$x1} {set x1 $x}
        if {$y<$y0} {set y0 $y}; if {$y>$y1} {set y1 $y}
      }
      set x0 [expr {$x0-$w}]; set x1 [expr {$x1+$w}]
      set y0 [expr {$y0-$w}]; set y1 [expr {$y1+$w}]
      foreach p $polys {
        foreach {x y} $p {
          set x [rnd $x]; set y [rnd $y]
          if {$x<$x0} {set x0 $x}; if {$x>$x1} {set x1 $x}
          if {$y<$y0} {set y0 $y}; if {$y>$y1} {set y1 $y}
        }
      }
      return [list [expr {$x0-1}] [expr {$y0-1}] [expr {$x1+1}] [expr {$y1+1}]]
    }
    rectangle - rect - oval {
      foreach {x0 y0 x1 y1} $c break
      set bloat [expr {int($width+1)/2}]
      if {$x1<$x0+1} {set x1 [expr {$x0+1}]}
      if {$y1<$y0+1} {set y1 [expr {$y0+1}]}
      set r {}
      foreach v [list $x0 $y0 $x1 $y1] s {-1 -1 1 1} {
        lappend r [expr {int($v>=0 ? $v+0.5 : $v-0.5) + $s*$bloat}]
      }
      return $r
    }
    arc {
      foreach {ax0 ay0 ax1 ay1 cx cy rx ry} [arc_geometry $id] break
      foreach {ox0 oy0 ox1 oy1} $c break
      set x0 [set x1 [expr {int($ax0)}]]
      set y0 [set y1 [expr {int($ay0)}]]
      set pts [list $ax1 $ay1]
      set start [dict get $opts($id) -start]
      set extent [dict get $opts($id) -extent]
      foreach a {0 90 180 270} px [list $ox1 $cx $ox0 $cx] \
                               py [list $cy $oy0 $cy $oy1] {
        set tmp [expr {$a-$start}]
        while {$tmp<0} {set tmp [expr {$tmp+360.0}]}
        while {$tmp>=360} {set tmp [expr {$tmp-360.0}]}
        if {$tmp<$extent || $tmp-360>$extent} {lappend pts $px $py}
      }
      foreach {x y} $pts {
        set x [rnd $x]; set y [rnd $y]
        if {$x<$x0} {set x0 $x}; if {$x>$x1} {set x1 $x}
        if {$y<$y0} {set y0 $y}; if {$y>$y1} {set y1 $y}
      }
      set w [expr {int(($width+1.0)/2.0+1)}]
      return [list [expr {$x0-$w}] [expr {$y0-$w}] \
                   [expr {$x1+$w}] [expr {$y1+$w}]]
    }
    text {
      foreach {l t w h} [text_box $id] break
      return [list $l $t [expr {$l+$w}] [expr {$t+$h}]]
    }
  }
  error "unknown item type: $type($id)"
}

proc cv::bbox {args} {
  set r {}
  foreach tagOrId $args {
    foreach id [find $tagOrId] {
      foreach {x0 y0 x1 y1} [item_bbox $id] break
      if {$r=={}} {
        set r [list $x0 $y0 $x1 $y1]
      } else {
        foreach {a b c d} $r break
        set r [list [expr {min($a,$x0)}] [expr {min($b,$y0)}] \
                    [expr {max($c,$x1)}] [expr {max($d,$y1)}]]
      }
    }
  }
  return $r
}

proc .c {op args} {
  return [cv::$op {*}$args]
}

source [file join $dir bubble-generator.tcl]

proc fmt {x} {
  set s [format %.2f $x]
  regsub {\.?0+$} $s {} s
  if {$s=="-0"} {set s 0}
  return $s
}

proc xml_escape {s} {
  return [string map {& &amp; < &lt; > &gt; \" &quot;} $s]
}

# Join path segments, wrapping lines at about 100 characters.
#
proc wrap_path {segs} {
  set lines {}
  set cur {}
  foreach seg $segs {
    if {$cur!="" && [string length $cur]+[string length $seg]>=100} {
      lappend lines $cur
      set cur {}
    }
    append cur [expr {$cur=="" ? "" : " "}] $seg
  }
  if {$cur!=""} {lappend lines $cur}
  return [join $lines \n]
}

# Render the items on the canvas as SVG.  All of the strokes go in a single
# path, and all of the arrowheads in another, to keep the output small.
# If $inline is set, the result is three lines:  the <svg> tag (which
# makes it an HTML block in Markdown), everything else, and </svg>.
#
proc canvas_to_svg {name width height {inline 0}} {
  set sw [expr {round($width*$::SCALE)}]
  set sh [expr {round($height*$::SCALE)}]
  set strokes {}
  set arrows {}
  set texts {}
  foreach id $cv::order {
    set c $cv::coords($id)
    set o $cv::opts($id)
    switch $cv::type($id) {
      line {
        foreach {c polys} [cv::line_geometry $id] break
        set d "M[fmt [lindex $c 0]],[fmt [lindex $c 1]]"
        foreach {x y} [lrange $c 2 end] {append d " L[fmt $x],[fmt $y]"}
        lappend strokes $d
        foreach p $polys {
          set d "M[fmt [lindex $p 0]],[fmt [lindex $p 1]]"
          foreach {x y} [lrange $p 2 end] {append d " L[fmt $x],[fmt $y]"}
          lappend arrows "$d Z"
        }
      }
      arc {
        foreach {x0 y0 x1 y1 cx cy rx ry} [cv::arc_geometry $id] break
        set extent [dict get $o -extent]
        set large [expr {abs($extent)>180 ? 1 : 0}]
        set sweep [expr {$extent<0 ? 1 : 0}]
        lappend strokes "M[fmt $x0],[fmt $y0] A[fmt $rx],[fmt $ry] 0\
 $large,$sweep [fmt $x1],[fmt $y1]"
      }
      oval {
        foreach {x0 y0 x1 y1} $c break
        set rx [fmt [expr {($x1-$x0)/2.0}]]
        set ry [fmt [expr {($y1-$y0)/2.0}]]
        set cy [fmt [expr {($y0+$y1)/2.0}]]
        lappend strokes "M[fmt $x0],$cy A$rx,$ry 0 1,0 [fmt $x1],$cy\
 A$rx,$ry 0 1,0 [fmt $x0],$cy Z"
      }
      rectangle - rect {
        foreach {x0 y0 x1 y1} $c break
        lappend strokes "M[fmt $x0],[fmt $y0] H[fmt $x1] V[fmt $y1]\
 H[fmt $x0] Z"
      }
      text {
        # The postscript output draws the text at the nominal font size,
        # centered vertically on the extent of "TXygqPZ".  The width is
        # pinned to that of Helvetica-Bold so that text in a wider
        # fallback font still fits in its bubble.
        foreach {x y} $c break
        set size [lindex [dict get $o -font] 1]
        set txt [dict get $o -text]
        set base [expr {$y+0.249*$size}]
        set len [expr {[string_width Helvetica-Bold $txt]*$size/1000.0}]
        lappend texts "<text x=\"[fmt $x]\" y=\"[fmt $base]\"\
 font-size=\"$size\" textLength=\"[fmt $len]\"\
 lengthAdjust=\"spacingAndGlyphs\">[xml_escape $txt]</text>"
      }
    }
  }
  if {$inline} {
    set strokes [join $strokes]
    set arrows [join $arrows]
  } else {
    set strokes [wrap_path $strokes]
    set arrows [wrap_path $arrows]
  }
  set body {}
  lappend body "<title>[xml_escape $name]</title>"
  lappend body "<rect width=\"100%\" height=\"100%\" fill=\"white\"/>"
  lappend body "<path fill=\"none\" stroke=\"black\" stroke-width=\"2\" d=\"$strokes\"/>"
  if {$arrows!=""} {
    lappend body "<path fill=\"black\" d=\"$arrows\"/>"
  }
  if {[llength $texts]} {
    lappend body "<g font-family=\"Helvetica, Arial, 'Liberation Sans', 'Nimbus Sans', sans-serif\"\
 font-weight=\"bold\" text-anchor=\"middle\">"
    set body [concat $body $texts]
    lappend body "</g>"
  }
  set svg "<svg xmlns=\"http://www.w3.org/2000/svg\"\
 width=\"$sw\" height=\"$sh\" viewBox=\"0 0 [fmt $width] [fmt $height]\">"
  if {$inline} {
    return "$svg\n[join $body {}]\n</svg>"
  }
  return "$svg\n[join $body \n]\n</svg>"
}

# Return the SVG for the diagram $name.
#
proc render_graph {name {inline 0}} {
  .c delete all
  draw_diagram "line bullet [list $::diagram_spec($name)] bullet"
  foreach {x0 y0 x1 y1} [.c bbox all] break
  .c move all [expr {2-$x0}] [expr {2-$y0}]
  foreach {x0 y0 x1 y1} [.c bbox all] break
  return [canvas_to_svg $name [expr {$x1+2}] [expr {$y1+2}] $inline]
}

proc wanted {name} {
  if {[llength $::names]} {
    return [expr {[lsearch -exact $::names $name]>=0}]
  }
  return [expr {![regexp {^X-} $name]}]
}

# Replace the diagrams inlined in Markdown file $path.
#
proc update_markdown {path} {
  set f [open $path]
  set old [read $f]
  close $f
  set new {}
  set rest $old
  set n 0
  while {[set i [string first "<svg" $rest]]>=0} {
    set j [string first "</svg>" $rest $i]
    if {$j<0} break
    set j [expr {$j+[string length "</svg>"]-1}]
    append new [string range $rest 0 [expr {$i-1}]]
    set block [string range $rest $i $j]
    set rest [string range $rest [expr {$j+1}] end]
    if {![regexp {^<svg[^>]*>\s*<title>([^<]*)</title>} $block -> name]
        || ![info exists ::diagram_spec($name)] || ![wanted $name]} {
      append new $block
      continue
    }
    set attrs {}
    foreach a {role aria-label} {
      if {[regexp [format {^<svg[^>]*\s%s="([^"]*)"} $a] $block -> v]} {
        append attrs "$a=\"$v\" "
      }
    }
    set svg [render_graph $name 1]
    append new "<svg $attrs[string range $svg 5 end]"
    lappend ::found $name
    incr n
  }
  append new $rest
  if {$new!=$old} {
    set f [open $path w]
    puts -nonewline $f $new
    close $f
    puts "$path: updated $n diagram(s)"
  }
}

proc markdown_files {path} {
  if {![file isdirectory $path]} {return [list $path]}
  set r {}
  foreach p [lsort [glob -nocomplain -directory $path *]] {
    if {[file isdirectory $p]} {
      set r [concat $r [markdown_files $p]]
    } elseif {[string match *.md $p]} {
      lappend r $p
    }
  }
  return $r
}

array set diagram_spec $all_graphs
set found {}
if {[llength $update_paths]} {
  foreach path $update_paths {
    foreach md [markdown_files $path] {update_markdown $md}
  }
} else {
  file mkdir $outdir
  foreach {name spec} $all_graphs {
    if {![wanted $name]} continue
    lappend found $name
    set f [open [file join $outdir $name.svg] w]
    puts $f [render_graph $name]
    close $f
  }
}
foreach name $names {
  if {[lsearch -exact $found $name]<0} {
    puts stderr "no such diagram: $name"
  }
}
