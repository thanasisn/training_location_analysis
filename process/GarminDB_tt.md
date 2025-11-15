---
title:  "Process"
date:   "2025-11-15 17:54 EET"
author: ""

output:
  bookdown::pdf_document2:
    number_sections:  yes
    fig_caption:      no
    keep_tex:         yes
    keep_md:          yes
    latex_engine:     xelatex
    toc:              yes
    toc_depth:        4
    fig_width:        8
    fig_height:       5
    dev:              cairo_pdf
  html_document:
    toc:             true
    number_sections: false
    fig_width:       6
    fig_height:      4
    keep_md:         no

header-includes:
  - \usepackage{fontspec}
  - \usepackage{xunicode}
  - \usepackage{xltxtra}
  - \usepackage{placeins}
  - \geometry{
     a4paper,
     left     = 25mm,
     right    = 25mm,
     top      = 30mm,
     bottom   = 30mm,
     headsep  = 3\baselineskip,
     footskip = 4\baselineskip
   }
  - \setmainfont[Scale=1.1]{Linux Libertine O}
---



```
## 
## == garmin_activities.db 
## 
##    |--  _attributes  -- 
##          timestamp
##           key
##           value
## 
##    |--  activities  -- 
##          activity_id
##           name
##           description
##           type
##           course_id
##           laps
##           sport
##           sub_sport
##           device_serial_number
##           self_eval_feel
##           self_eval_effort
##           training_load
##           training_effect
##           anaerobic_training_effect
##           start_time
##           stop_time
##           elapsed_time
##           moving_time
##           distance
##           cycles
##           avg_hr
##           max_hr
##           avg_rr
##           max_rr
##           calories
##           avg_cadence
##           max_cadence
##           avg_speed
##           max_speed
##           ascent
##           descent
##           max_temperature
##           min_temperature
##           avg_temperature
##           start_lat
##           start_long
##           stop_lat
##           stop_long
##           hr_zones_method
##           hrz_1_hr
##           hrz_2_hr
##           hrz_3_hr
##           hrz_4_hr
##           hrz_5_hr
##           hrz_1_time
##           hrz_2_time
##           hrz_3_time
##           hrz_4_time
##           hrz_5_time
## 
##    |--  activities_devices  -- 
##          activity_id
##           device_serial_number
## 
##    |--  activity_laps  -- 
##          activity_id
##           lap
##           start_time
##           stop_time
##           elapsed_time
##           moving_time
##           distance
##           cycles
##           avg_hr
##           max_hr
##           avg_rr
##           max_rr
##           calories
##           avg_cadence
##           max_cadence
##           avg_speed
##           max_speed
##           ascent
##           descent
##           max_temperature
##           min_temperature
##           avg_temperature
##           start_lat
##           start_long
##           stop_lat
##           stop_long
##           hr_zones_method
##           hrz_1_hr
##           hrz_2_hr
##           hrz_3_hr
##           hrz_4_hr
##           hrz_5_hr
##           hrz_1_time
##           hrz_2_time
##           hrz_3_time
##           hrz_4_time
##           hrz_5_time
## 
##    |--  activity_records  -- 
##          activity_id
##           record
##           timestamp
##           position_lat
##           position_long
##           distance
##           cadence
##           altitude
##           hr
##           rr
##           speed
##           temperature
## 
##    |--  activity_splits  -- 
##          activity_id
##           split
##           grade
##           completed
##           falls
##           start_time
##           stop_time
##           elapsed_time
##           moving_time
##           distance
##           cycles
##           avg_hr
##           max_hr
##           avg_rr
##           max_rr
##           calories
##           avg_cadence
##           max_cadence
##           avg_speed
##           max_speed
##           ascent
##           descent
##           max_temperature
##           min_temperature
##           avg_temperature
##           start_lat
##           start_long
##           stop_lat
##           stop_long
##           hr_zones_method
##           hrz_1_hr
##           hrz_2_hr
##           hrz_3_hr
##           hrz_4_hr
##           hrz_5_hr
##           hrz_1_time
##           hrz_2_time
##           hrz_3_time
##           hrz_4_time
##           hrz_5_time
## 
##    |--  climbing_activities  -- 
##          total_routes
##           activity_id
## 
##    |--  climbing_activities_view  -- 
##          activity_id
##           name
##           description
##           sub_sport
##           start_time
##           stop_time
##           elapsed_time
##           moving_time
##           avg_hr
##           max_hr
##           calories
##           ascent
##           descent
##           total_routes
##           training_effect
##           anaerobic_training_effect
##           heart_rate_zone_one_time
##           heart_rate_zone_two_time
##           heart_rate_zone_three_time
##           heart_rate_zone_four_time
##           heart_rate_zone_five_time
## 
##    |--  cycle_activities  -- 
##          strokes
##           vo2_max
##           activity_id
## 
##    |--  cycle_activities_view  -- 
##          activity_id
##           name
##           description
##           sub_sport
##           start_time
##           stop_time
##           elapsed_time
##           distance
##           strokes
##           avg_hr
##           max_hr
##           avg_rr
##           max_rr
##           calories
##           avg_temperature
##           avg_rpms
##           max_rpms
##           avg_speed
##           max_speed
##           vo2_max
##           training_effect
##           anaerobic_training_effect
##           heart_rate_zone_one_time
##           heart_rate_zone_two_time
##           heart_rate_zone_three_time
##           heart_rate_zone_four_time
##           heart_rate_zone_five_time
##           start_loc
##           stop_loc
## 
##    |--  hiking_activities_view  -- 
##          activity_id
##           name
##           description
##           start_time
##           stop_time
##           elapsed_time
##           distance
##           steps
##           avg_pace
##           avg_moving_pace
##           max_pace
##           avg_steps_per_min
##           max_steps_per_min
##           avg_hr
##           max_hr
##           calories
##           avg_temperature
##           avg_speed
##           max_speed
##           avg_step_length
##           vo2_max
##           training_effect
##           anaerobic_training_effect
##           heart_rate_zone_one_time
##           heart_rate_zone_two_time
##           heart_rate_zone_three_time
##           heart_rate_zone_four_time
##           heart_rate_zone_five_time
##           start_loc
##           stop_loc
## 
##    |--  paddle_activities  -- 
##          strokes
##           avg_stroke_distance
##           activity_id
## 
##    |--  paddle_activities_view  -- 
##          activity_id
##           name
##           description
##           sport
##           sub_sport
##           start_time
##           stop_time
##           elapsed_time
##           distance
##           strokes
##           avg_stroke_distance
##           avg_cadence
##           max_cadence
##           avg_hr
##           max_hr
##           calories
##           avg_temperature
##           avg_speed
##           max_speed
##           training_effect
##           anaerobic_training_effect
##           start_loc
##           stop_loc
## 
##    |--  running_activities_view  -- 
##          activity_id
##           name
##           description
##           sub_sport
##           course_id
##           start_time
##           stop_time
##           elapsed_time
##           distance
##           steps
##           avg_pace
##           avg_moving_pace
##           max_pace
##           avg_steps_per_min
##           max_steps_per_min
##           avg_hr
##           max_hr
##           avg_rr
##           max_rr
##           calories
##           avg_temperature
##           avg_speed
##           max_speed
##           avg_step_length
##           avg_vertical_ratio
##           avg_gct_balance
##           avg_vertical_oscillation
##           avg_ground_contact_time
##           avg_stance_time_percent
##           vo2_max
##           training_effect
##           anaerobic_training_effect
##           heart_rate_zone_one_time
##           heart_rate_zone_two_time
##           heart_rate_zone_three_time
##           heart_rate_zone_four_time
##           heart_rate_zone_five_time
##           start_loc
##           stop_loc
## 
##    |--  steps_activities  -- 
##          steps
##           avg_pace
##           avg_moving_pace
##           max_pace
##           avg_steps_per_min
##           max_steps_per_min
##           avg_step_length
##           avg_vertical_ratio
##           avg_vertical_oscillation
##           avg_gct_balance
##           avg_ground_contact_time
##           avg_stance_time_percent
##           vo2_max
##           activity_id
## 
##    |--  steps_activities_view  -- 
##          activity_id
##           name
##           description
##           sport
##           sub_sport
##           type
##           course_id
##           start_time
##           stop_time
##           elapsed_time
##           distance
##           steps
##           avg_pace
##           avg_moving_pace
##           max_pace
##           avg_steps_per_min
##           max_steps_per_min
##           avg_hr
##           max_hr
##           calories
##           avg_temperature
##           avg_speed
##           max_speed
##           avg_step_length
##           vo2_max
##           training_effect
##           anaerobic_training_effect
##           heart_rate_zone_one_time
##           heart_rate_zone_two_time
##           heart_rate_zone_three_time
##           heart_rate_zone_four_time
##           heart_rate_zone_five_time
##           start_loc
##           stop_loc
## 
##    |--  walking_activities_view  -- 
##          activity_id
##           name
##           description
##           start_time
##           stop_time
##           elapsed_time
##           distance
##           steps
##           avg_pace
##           avg_moving_pace
##           max_pace
##           avg_steps_per_min
##           max_steps_per_min
##           avg_hr
##           max_hr
##           calories
##           avg_temperature
##           avg_speed
##           max_speed
##           avg_step_length
##           vo2_max
##           training_effect
##           anaerobic_training_effect
##           heart_rate_zone_one_time
##           heart_rate_zone_two_time
##           heart_rate_zone_three_time
##           heart_rate_zone_four_time
##           heart_rate_zone_five_time
##           start_loc
##           stop_loc
## 
## == garmin_monitoring.db 
## 
##    |--  _attributes  -- 
##          timestamp
##           key
##           value
## 
##    |--  monitoring  -- 
##          timestamp
##           activity_type
##           intensity
##           duration
##           distance
##           cum_active_time
##           active_calories
##           steps
##           strokes
##           cycles
## 
##    |--  monitoring_climb  -- 
##          timestamp
##           ascent
##           descent
##           cum_ascent
##           cum_descent
## 
##    |--  monitoring_hr  -- 
##          timestamp
##           heart_rate
## 
##    |--  monitoring_info  -- 
##          timestamp
##           file_id
##           activity_type
##           resting_metabolic_rate
##           cycles_to_distance
##           cycles_to_calories
## 
##    |--  monitoring_intensity  -- 
##          timestamp
##           moderate_activity_time
##           vigorous_activity_time
## 
##    |--  monitoring_pulse_ox  -- 
##          timestamp
##           pulse_ox
## 
##    |--  monitoring_rr  -- 
##          timestamp
##           rr
## 
## == garmin_summary.db 
## 
##    |--  _attributes  -- 
##          timestamp
##           key
##           value
## 
##    |--  days_summary  -- 
##          day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  days_summary_view  -- 
##          day
##           hr_avg
##           hr_min
##           hr_max
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           steps
##           steps_goal_percent
##           floors
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  intensity_hr  -- 
##          timestamp
##           intensity
##           heart_rate
## 
##    |--  months_summary  -- 
##          first_day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  months_summary_view  -- 
##          first_day
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           total_steps
##           steps_avg
##           steps_goal_percent
##           total_floors
##           floors_avg
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           sweat_loss_avg:1
##           spo2_avg
##           rr_waking_avg
## 
##    |--  summary  -- 
##          timestamp
##           key
##           value
## 
##    |--  weeks_summary  -- 
##          first_day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  weeks_summary_view  -- 
##          first_day
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           total_steps
##           steps_avg
##           steps_goal_percent
##           total_floors
##           floors_avg
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           sweat_loss_avg:1
##           spo2_avg
##           rr_waking_avg
## 
##    |--  years_summary  -- 
##          first_day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  years_summary_view  -- 
##          first_day
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           total_steps
##           steps_avg
##           steps_goal_percent
##           total_floors
##           floors_avg
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           sweat_loss_avg:1
##           spo2_avg
##           rr_waking_avg
## 
## == garmin.db 
## 
##    |--  _attributes  -- 
##          timestamp
##           key
##           value
## 
##    |--  attributes  -- 
##          timestamp
##           key
##           value
## 
##    |--  daily_summary  -- 
##          day
##           hr_min
##           hr_max
##           rhr
##           stress_avg
##           step_goal
##           steps
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           floors_up
##           floors_down
##           floors_goal
##           distance
##           calories_goal
##           calories_total
##           calories_bmr
##           calories_active
##           calories_consumed
##           hydration_goal
##           hydration_intake
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_charged
##           bb_max
##           bb_min
##           description
## 
##    |--  device_info  -- 
##          timestamp
##           file_id
##           serial_number
##           software_version
##           cum_operating_time
##           battery_status
##           battery_voltage
## 
##    |--  device_info_view  -- 
##          timestamp
##           file_id
##           serial_number
##           device_type
##           software_version
##           manufacturer
##           product
##           hardware_version
##           battery_status
## 
##    |--  devices  -- 
##          serial_number
##           timestamp
##           device_type
##           manufacturer
##           product
##           hardware_version
## 
##    |--  files  -- 
##          id
##           name
##           type
##           serial_number
## 
##    |--  files_view  -- 
##          timestamp
##           activity_id
##           name
##           type
##           manufacturer
##           product
##           serial_number
## 
##    |--  resting_hr  -- 
##          day
##           resting_heart_rate
## 
##    |--  sleep  -- 
##          day
##           start
##           end
##           total_sleep
##           deep_sleep
##           light_sleep
##           rem_sleep
##           awake
##           avg_spo2
##           avg_rr
##           avg_stress
##           score
##           qualifier
## 
##    |--  sleep_events  -- 
##          timestamp
##           event
##           duration
## 
##    |--  stress  -- 
##          timestamp
##           stress
## 
##    |--  weight  -- 
##          day
##           weight
## 
## == summary.db 
## 
##    |--  _attributes  -- 
##          timestamp
##           key
##           value
## 
##    |--  days_summary  -- 
##          day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  days_summary_view  -- 
##          day
##           hr_avg
##           hr_min
##           hr_max
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           steps
##           steps_goal_percent
##           floors
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  months_summary  -- 
##          first_day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  months_summary_view  -- 
##          first_day
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           total_steps
##           steps_avg
##           steps_goal_percent
##           total_floors
##           floors_avg
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           sweat_loss_avg:1
##           spo2_avg
##           rr_waking_avg
## 
##    |--  summary  -- 
##          timestamp
##           key
##           value
## 
##    |--  weeks_summary  -- 
##          first_day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  weeks_summary_view  -- 
##          first_day
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           total_steps
##           steps_avg
##           steps_goal_percent
##           total_floors
##           floors_avg
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           sweat_loss_avg:1
##           spo2_avg
##           rr_waking_avg
## 
##    |--  years_summary  -- 
##          first_day
##           hr_avg
##           hr_min
##           hr_max
##           rhr_avg
##           rhr_min
##           rhr_max
##           inactive_hr_avg
##           inactive_hr_min
##           inactive_hr_max
##           weight_avg
##           weight_min
##           weight_max
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           intensity_time_goal
##           steps
##           steps_goal
##           floors
##           floors_goal
##           sleep_avg
##           sleep_min
##           sleep_max
##           rem_sleep_avg
##           rem_sleep_min
##           rem_sleep_max
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_goal
##           calories_consumed_avg
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           hydration_intake
##           sweat_loss_avg
##           sweat_loss
##           spo2_avg
##           spo2_min
##           rr_waking_avg
##           rr_max
##           rr_min
##           bb_max
##           bb_min
## 
##    |--  years_summary_view  -- 
##          first_day
##           rhr
##           inactive_hr
##           weight
##           intensity_time
##           moderate_activity_time
##           vigorous_activity_time
##           total_steps
##           steps_avg
##           steps_goal_percent
##           total_floors
##           floors_avg
##           floors_goal_percent
##           sleep_avg
##           rem_sleep_avg
##           stress_avg
##           calories_avg
##           calories_bmr_avg
##           calories_active_avg
##           calories_consumed_avg
##           calories_goal
##           activities
##           activities_calories
##           activities_distance
##           hydration_goal
##           hydration_avg
##           sweat_loss_avg
##           sweat_loss_avg:1
##           spo2_avg
##           rr_waking_avg
```

```
## 
##    |--  _attributes  -- 
##          timestamp
##           key
##           value
## 
##    |--  monitoring  -- 
##          timestamp
##           activity_type
##           intensity
##           duration
##           distance
##           cum_active_time
##           active_calories
##           steps
##           strokes
##           cycles
## 
##    |--  monitoring_climb  -- 
##          timestamp
##           ascent
##           descent
##           cum_ascent
##           cum_descent
## 
##    |--  monitoring_hr  -- 
##          timestamp
##           heart_rate
## 
##    |--  monitoring_info  -- 
##          timestamp
##           file_id
##           activity_type
##           resting_metabolic_rate
##           cycles_to_distance
##           cycles_to_calories
## 
##    |--  monitoring_intensity  -- 
##          timestamp
##           moderate_activity_time
##           vigorous_activity_time
## 
##    |--  monitoring_pulse_ox  -- 
##          timestamp
##           pulse_ox
## 
##    |--  monitoring_rr  -- 
##          timestamp
##           rr
```

```
## _attributes No rows
```

![](GarminDB_tt_files/figure-latex/unnamed-chunk-2-1.pdf)<!-- --> 

**END**


```
## 2025-11-15 15:54:36.082975 athan@mumra GarminDB_tt.R 0.212892 mins
```

