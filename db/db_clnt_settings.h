#ifndef _DB_CLNT_SETTINGS_H
#define _DB_CLNT_SETTINGS_H

// REGISTER_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT)
REGISTER_ACC_SETTING(dbtran, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);

REGISTER_SETTING(dbtran.mode, "", SETTING_ENUM, SETFLAG_DERIVED, gbl_setting_default_query_timeout);

REGISTER_SETTING(dbtran.maxchunksize, "", SETTING_ENUM, SETFLAG_DERIVED, gbl_setting_default_query_timeout);

// How do I handle cln->plugin.set_timeout?
REGISTER_ACC_SETTING(plugin, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);
REGISTER_SETTING(plugin.set_timeout, "", SETTING_FUNC, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(query_timeout, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(tzname, "", SETTING_STRING, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(dtprec, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);

REGISTER_ACC_SETTING(current_user, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);
REGISTER_SETTING(current_user.name, "", SETTING_STRING, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_SETTING(current_user.is_x509_user, "", SETTING_INTEGER, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_SETTING(current_user.have_password, "", SETTING_INTEGER, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(authgen, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);

REGISTER_ACC_SETTING(spname, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_SETTING(spversion.version_num, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_SETTING(spversion.version_str, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(want_stored_procedure_trace, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(bdb_osql_trak, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(verifyretry_off, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(statement_query_effects, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(get_cost, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(is_explain, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(osql_max_trans, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(group_concat_mem_limit, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(planner_effort, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(appdata, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(admin, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(is_readonly, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(is_expert, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(is_fast_expert, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);

#endif
