/* These were not ported over to SQLite 3.7.2 */
#if 0
DEF_ATTR(IN_CLAUSE_MULTIPLIER, in_clause_multiplier, QUANTITY, 16)
DEF_ATTR(DISABLE_IN_MULTIPLIER_PENALTY, disable_in_multiplier_pentalty, BOOLEAN, 1)
#endif

DEF_ATTR(SCALE_IN_CLAUSE, scale_in_clause, BOOLEAN, 1)
/* stat4 number of samples will multiply with this value*/
DEF_ATTR(STAT4_SAMPLES_MULTIPLIER, stat4_samples_multiplier, QUANTITY, 0)
/* stat4 number of samples more than the default 24 */
DEF_ATTR(STAT4_EXTRA_SAMPLES, stat4_extra_samples, QUANTITY, 0)
/* build sqlite_stat1 table entries for empty tables */
DEF_ATTR(ANALYZE_EMPTY_TABLES, analyze_empty_tables, BOOLEAN, 0)
