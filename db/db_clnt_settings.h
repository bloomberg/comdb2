#ifndef _DB_CLNT_SETTINGS_H
#define _DB_CLNT_SETTINGS_H

// REGISTER_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT)
REGISTER_ACC_SETTING(dbtran.mode, mode, SETTING_INTEGER, 0, gbl_setting_default_mode);
REGISTER_ACC_SETTING(dbtran.maxchunksize, chunk, SETTING_INTEGER, 0, gbl_setting_default_query_timeout);

// How do I handle cln->plugin.set_timeout?
REGISTER_ACC_SETTING(plugin.set_timeout, timeout, SETTING_INTEGER, SETFLAG_WRITEONLY,
                     gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(query_timeout, maxquerytime, SETTING_LONG, 0, gbl_setting_default_query_timeout)
REGISTER_ACC_SETTING(tzname, timezone, SETTING_CSTRING, 0, gbl_setting_default_timezone);
REGISTER_ACC_SETTING(dtprec, datetime, SETTING_INTEGER, 0, gbl_setting_default_timezone);

REGISTER_ACC_SETTING(current_user.name, user, SETTING_CSTRING, 0, gbl_setting_default_user);
REGISTER_ACC_SETTING(current_user.password, password, SETTING_CSTRING, SETFLAG_INTERNAL, gbl_setting_default_password);
REGISTER_ACC_SETTING(spversion.version_str, spversion, SETTING_STRING, 0, gbl_setting_default_spversion);
REGISTER_ACC_SETTING(prepare_only, prepare_only, SETTING_INTEGER, 0, gbl_setting_default_prepare_only);
REGISTER_ACC_SETTING(is_readonly, readonly, SETTING_INTEGER, 0, gbl_setting_default_readonly);
REGISTER_ACC_SETTING(is_expert, expert, SETTING_INTEGER, 0, gbl_setting_default_expert);
REGISTER_ACC_SETTING(want_stored_procedure_trace, sptrace, SETTING_INTEGER, 0, gbl_setting_default_sptrace);
REGISTER_ACC_SETTING(bdb_osql_trak, cursordebug, SETTING_INTEGER, 0, gbl_setting_default_cursordebug);
REGISTER_ACC_SETTING(want_stored_procedure_debug, spdebug, SETTING_INTEGER, 0, gbl_setting_default_spdebug);
REGISTER_ACC_SETTING(hasql_on, hasql, SETTING_INTEGER, 0, gbl_setting_default_hasql);
REGISTER_ACC_SETTING(verifyretry_off, verifyretry, SETTING_INTEGER, 0, gbl_setting_default_verifyretry);
REGISTER_ACC_SETTING(statement_query_effects, queryeffects, SETTING_INTEGER, 0, gbl_setting_default_queryeffects);
REGISTER_ACC_SETTING(fdb_state.access, remote, SETTING_STRUCT, 0, gbl_setting_default_remote);
REGISTER_ACC_SETTING(get_cost, getcost, SETTING_INTEGER, 0, gbl_setting_default_getcost);
REGISTER_ACC_SETTING(is_explain, explain, SETTING_INTEGER, 0, gbl_setting_default_explain);
REGISTER_ACC_SETTING(osql_max_trans, maxtransize, SETTING_LONG, 0, gbl_setting_default_maxtransize);
REGISTER_ACC_SETTING(group_concat_mem_limit, groupconcatmemlimit, SETTING_LONG, 0,
                     gbl_setting_default_groupconcatmemlimit);
REGISTER_ACC_SETTING(planner_effort, plannereffort, SETTING_INTEGER, 0, gbl_setting_default_plannereffort);
REGISTER_ACC_SETTING(appdata, intransresults, SETTING_STRING, 0, gbl_setting_default_intransresults);
REGISTER_ACC_SETTING(admin, admin, SETTING_INTEGER, 0, gbl_setting_default_admin);
REGISTER_ACC_SETTING(limits, querylimit, SETTING_MULTIPLE, SETFLAG_WRITEONLY, gbl_setting_default_querylimit);
REGISTER_ACC_SETTING(rowbuffer, rowbuffer, SETTING_INTEGER, 0, gbl_setting_default_rowbuffer);
REGISTER_ACC_SETTING(begin, sockbplog, SETTING_MULTIPLE, 0, gbl_setting_default_sockbplog);

#endif
