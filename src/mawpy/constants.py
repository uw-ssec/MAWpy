USER_ID = 'user_id'
STAY = 'stay'
STAY_DUR = 'stay_dur'
STAY_LAT = 'stay_lat'
STAY_LONG = 'stay_long'
STAY_UNC = 'stay_unc'
STAY_LAT_PRE_COMBINED = 'stay_lat_pre_combined'
STAY_LONG_PRE_COMBINED = 'stay_long_pre_combined'
STAY_PRE_COMBINED = 'stay_pre_combined'
ORIG_LAT = 'orig_lat'
ORIG_LONG = 'orig_long'
ORIG_UNC = 'orig_unc'
UNIX_START_T = 'unix_start_t'
UNIX_START_DATE = 'unix_start_date'

STAY_LAT_LONG = [STAY_LAT, STAY_LONG]

IC_COLUMNS = [USER_ID, UNIX_START_T, UNIX_START_DATE,
              ORIG_LAT, ORIG_LONG, ORIG_UNC,
              STAY_LAT, STAY_LONG, STAY_UNC,
              STAY_DUR, STAY]

TSC_COLUMNS = [USER_ID, UNIX_START_T, UNIX_START_DATE,
               ORIG_LAT, ORIG_LONG, ORIG_UNC,
               STAY_LAT, STAY_LONG,
               STAY_DUR, STAY]
