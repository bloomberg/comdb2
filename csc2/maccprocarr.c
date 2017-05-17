#include <stdio.h>
#include <string.h>
#include "macc.h"

extern int dims[7];
extern char *dims_cnst[7];
extern int ranges[2];
extern int range_or_array;
extern int declaration;
extern int range;

int process_array_(int dim[6], int rg[2], char *buf, int *arr, char *dim_cn[6])
{
    int i, count = 0, fortran = 0;
    /*fprintf(stderr, "RANGE OR ARR: %d\n", range_or_array);*/
    memset(dim, -1, sizeof(int) * 6);
    memset(rg, 0, sizeof(int) * 2);
    (*arr) = -1;

    switch (range_or_array) {
    case 1:
        if (declaration)
            (*arr) = 1;
        if (!declaration)
            fortran = 1;
    case 3:
        /* for fortran array, we reverse the order of the dimensions */
        /* Since C array parsed in reverse way, we also have to reverse C-array
         * elements */
        for (i = 0; i < 6 && (dims[i] != -1); i++) {
            count++;
        }

        /* because of the way the parsing is done for fortran array, the first
           array
           dimension is stored in the last element of dims[]
           */
        if (count > 0) {
            dim[count - 1] = dims[count - 1] - fortran;
            if (dim_cn != NULL)
                dim_cn[count - 1] = (char *)(dims_cnst[count - 1]);
        }

        if (range_or_array == 1) {
            for (i = 0; i < count - 1; i++) {
                dim[i] = dims[count - i - 2] - fortran;
                if (dim_cn != NULL)
                    dim_cn[i] = (char *)dims_cnst[count - i - 2];
            }
        } else {
            for (i = count - 1; i >= 0; i--) {
                dim[count - i - 1] = dims[i] - fortran;
                if (dim_cn != NULL)
                    dim_cn[count - i - 1] = (char *)dims_cnst[i];
            }
        }

        if (declaration && (*arr) != 1)
            (*arr) = 0;
        break;
    }
    fortran = 0;
    switch (range) {
    case CLANG:
        fortran = 1;
    case FORTRAN:
        if (ranges[0] == -2)
            rg[0] = -1;
        else
            rg[0] = ranges[0] + fortran;
        if (ranges[1] == -2)
            rg[1] = -1;
        else
            rg[1] = ranges[1] + fortran;

        if (rg[0] <= 0)
            rg[0] = -1;
        if (rg[1] <= 0)
            rg[1] = -1;
        break;
    }

    /*fprintf(stderr, "DIMENSIONS: ");
      for (i=0;i<6;i++)
        fprintf(stderr, "{%d,%d} ", dims[i], dim[i]);
      fprintf(stderr, "\n");

      fprintf(stderr, "RAGNES: ");
      for (i=0;i<2;i++)
        fprintf(stderr, "{%d,%d} ", ranges[i], rg[i]);
      fprintf(stderr, "\n");*/
    return 0;
}
