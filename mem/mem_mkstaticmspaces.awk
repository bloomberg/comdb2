#########################################
# Definitions needed by static mspaces  #
#########################################
BEGIN {
    cnt=0;
    array = "{\"INVALID\", 0, 0},\n";
    printf "#ifndef INCLUDED_MEM_INT_H\n";
    printf "#define INCLUDED_MEM_INT_H\n";
}
{
    if(substr($1, 1, 1) != "#") {
        ++cnt;
        printf "#define COMDB2MA_STATIC_%s %d\n", toupper($2), cnt;
        array = array"{\""$2"\", "$3", "$4"},\n";
    }
}
END {
    ++cnt;
    printf "#define COMDB2MA_COUNT %d\n", cnt;
    printf "static comdb2ma COMDB2_STATIC_MAS[%d];\n", cnt;
    array = array"{NULL, 0, 0}";
    ++cnt;
    printf "static struct {char *name; size_t size; size_t cap;} COMDB2_STATIC_MA_METAS[%d] = {\n", cnt;
    print array;
    printf "};\n";
    printf "#endif\n";
}
