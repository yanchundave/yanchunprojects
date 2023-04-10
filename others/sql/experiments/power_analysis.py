import numpy as np
from scipy import stats
import statsmodels.stats.api as sms
from statsmodels.stats.power import TTestIndPower
# get power
# _c_size: sample size of control
# _c_pos: positive count in control
# _t_size: sample size of treatment
# _t_pos: positive count in treatment
def get_power(_c_size, _c_pos, _t_size, _t_pos):
    positive_population = _c_pos + _t_pos
    num_population = _c_size + _t_size
    population_data = np.concatenate(
        (np.ones(positive_population), np.zeros(num_population - positive_population)), axis=None)
    population_mean = np.mean(population_data)
    population_std = np.std(population_data)
    # print('Population mean: {:.4f}, and std: {:.4f}'.format(population_mean, population_std))

    used_std = population_std
    effect_size = (_c_pos * 1.0 / _c_size - _t_pos  * 1.0 / _t_size) / used_std

    return(TTestIndPower().power(effect_size,
                              _c_size,
                              0.05,
                              _t_size*1.0/_c_size,
                              _c_size + _t_size - 2,
                              'smaller')
                              )

# get pvalue, delta, and confidence interval
# _c_size: sample size of control
# _c_pos: positive count in control
# _t_size: sample size of treatment
# _t_pos: positive count in treatment
def get_pvalue_ci(_c_size, _c_pos, _t_size, _t_pos):
    control_data = np.concatenate((np.ones(_c_pos), np.zeros(_c_size - _c_pos)), axis=None)
    treatment_data = np.concatenate((np.ones(_t_pos), np.zeros(_t_size - _t_pos)), axis=None)

    control_mean = np.mean(control_data)
    # control_std = np.std(control_data)
    treatment_mean = np.mean(treatment_data)
    # treatment_std = np.std(treatment_data)

    delta = treatment_mean - control_mean

    # print('Control mean: {:.4f}, and std: {:.4f}'.format(control_mean, control_std))
    # print('Treatment mean: {:.4f}, and std: {:.4f}'.format(treatment_mean, treatment_std))

    # calculate p-value
    t_stats, p_value = stats.ttest_ind(control_data, treatment_data, equal_var=False)

    # calculate confidence interval
    cm = sms.CompareMeans(sms.DescrStatsW(treatment_data), sms.DescrStatsW(control_data))
    ci = cm.tconfint_diff(usevar='unequal')

    return (p_value, delta, ci)

cohort_name_list = ['cohort 1: non-converted PV',
  'cohort 2: deep dormant',
  'cohort 3: opted into CBv1',
  'cohort 4: active users, not opted into CBv1',
  'cohort 5: new PV'
  ]

df_data = datasets['Query 1']

df_1st_advance_groupby = df_data.groupby(by=['COHORT', 'TEST_VARIANT']).agg({'USER_CNT': 'sum', 'ADVANCE_TAKEN_USER_CNT': 'sum'}).reset_index()
df_1st_advance_groupby['first_taken_rate'] = df_1st_advance_groupby['ADVANCE_TAKEN_USER_CNT'] / df_1st_advance_groupby['USER_CNT']
df_1st_advance_pivot = df_1st_advance_groupby.pivot(index='COHORT', columns = 'TEST_VARIANT', values=['USER_CNT', 'ADVANCE_TAKEN_USER_CNT', 'first_taken_rate']).reset_index()

# first advance rate
for cohort_name in cohort_name_list:
  cohort_of_interest = df_1st_advance_pivot[df_1st_advance_pivot['COHORT'] == cohort_name]

  num_control = int(cohort_of_interest['USER_CNT']['"control"']) # control size
  num_treatment = int(cohort_of_interest['USER_CNT']['"enabled"']) # treatment size
  positive_control = int(cohort_of_interest['ADVANCE_TAKEN_USER_CNT']['"control"']) # control positive
  positive_treatment = int(cohort_of_interest['ADVANCE_TAKEN_USER_CNT']['"enabled"']) # treatment positive

  power = get_power(num_control, positive_control, num_treatment, positive_treatment)
  p_value, delta, ci = get_pvalue_ci(num_control, positive_control, num_treatment, positive_treatment)
  is_significant = p_value < 0.05
  is_power_enough = power > 0.8
  print('\n--------------------------------------')
  print(cohort_name)
  print('stats-sig!' if is_significant else 'NOT stats-sig!')
  print('power is enough!' if is_power_enough else 'Under power!')
  print('p_value: {:.3f} (<0.05 to be stats-sig) '.format(p_value))
  # print('Power is {:.4f}'.format(power))
  print('Power is {:.1%} (80% is needded) '.format(power))
  print()
  # print('Delta (treatment - control): {:.3f}'.format(delta))
  print('Delta (treatment - control): {:.1%}'.format(delta))
  print('Confidence Interval of Delta : ({:.1%}, {:.1%})'.format(ci[0], ci[1]))
  # print('Confidence Interval of Delta', ci)
  df_payoff_groupby = df_data.groupby(by=['COHORT', 'TEST_VARIANT']).agg({'ADVANCE_TAKEN_USER_CNT': 'sum', 'PAYOFF_USER_CNT': 'sum'}).reset_index()
df_payoff_groupby['payoff_rate'] = df_payoff_groupby['PAYOFF_USER_CNT'] / df_payoff_groupby['ADVANCE_TAKEN_USER_CNT']
df_payoff_pivot = df_payoff_groupby.pivot(index='COHORT', columns = 'TEST_VARIANT', values=['ADVANCE_TAKEN_USER_CNT', 'PAYOFF_USER_CNT', 'payoff_rate']).reset_index()

# payoff rate
for cohort_name in cohort_name_list:
  cohort_of_interest = df_payoff_pivot[df_1st_advance_pivot['COHORT'] == cohort_name]

  num_control = int(cohort_of_interest['ADVANCE_TAKEN_USER_CNT']['"control"']) # control size
  num_treatment = int(cohort_of_interest['ADVANCE_TAKEN_USER_CNT']['"enabled"']) # treatment size
  positive_control = int(cohort_of_interest['PAYOFF_USER_CNT']['"control"']) # control positive
  positive_treatment = int(cohort_of_interest['PAYOFF_USER_CNT']['"enabled"']) # treatment positive

  power = get_power(num_control, positive_control, num_treatment, positive_treatment)
  p_value, delta, ci = get_pvalue_ci(num_control, positive_control, num_treatment, positive_treatment)
  is_significant = p_value < 0.05
  is_power_enough = power > 0.8
  print('\n--------------------------------------')
  print(cohort_name)
  print('stats-sig!' if is_significant else 'NOT stats-sig!')
  # print('power is enough!' if is_power_enough else 'Under power!')
  print('p_value: {:.3f} (<0.05 to be stats-sig) '.format(p_value))
  # print('Power is {:.4f}'.format(power))
  print('Power is {:.1%} (80% is needded) '.format(power))
  print()
  # print('Delta (treatment - control): {:.3f}'.format(delta))
  print('Delta (treatment - control): {:.1%}'.format(delta))
  print('Confidence Interval of Delta : ({:.1%}, {:.1%})'.format(ci[0], ci[1]))
  # print('Confidence Interval of Delta', ci)
  df_2nd_advance_groupby = df_data.groupby(by=['COHORT', 'TEST_VARIANT']).agg({'ADVANCE_TAKEN_USER_CNT': 'sum', 'SEC_ADVANCE_USER_CNT': 'sum'}).reset_index()
df_2nd_advance_groupby['payoff_rate'] = df_2nd_advance_groupby['SEC_ADVANCE_USER_CNT'] / df_2nd_advance_groupby['ADVANCE_TAKEN_USER_CNT']
df_2nd_advance_pivot = df_2nd_advance_groupby.pivot(index='COHORT', columns = 'TEST_VARIANT', values=['ADVANCE_TAKEN_USER_CNT', 'SEC_ADVANCE_USER_CNT', 'payoff_rate']).reset_index()

# 2nd advance rate
for cohort_name in cohort_name_list:
  cohort_of_interest = df_2nd_advance_pivot[df_1st_advance_pivot['COHORT'] == cohort_name]

  num_control = int(cohort_of_interest['ADVANCE_TAKEN_USER_CNT']['"control"']) # control size
  num_treatment = int(cohort_of_interest['ADVANCE_TAKEN_USER_CNT']['"enabled"']) # treatment size
  positive_control = int(cohort_of_interest['SEC_ADVANCE_USER_CNT']['"control"']) # control positive
  positive_treatment = int(cohort_of_interest['SEC_ADVANCE_USER_CNT']['"enabled"']) # treatment positive

  power = get_power(num_control, positive_control, num_treatment, positive_treatment)
  p_value, delta, ci = get_pvalue_ci(num_control, positive_control, num_treatment, positive_treatment)
  is_significant = p_value < 0.05
  is_power_enough = power > 0.8
  print('\n--------------------------------------')
  print(cohort_name)
  print('stats-sig!' if is_significant else 'NOT stats-sig!')
  # print('power is enough!' if is_power_enough else 'Under power!')
  print('p_value: {:.3f} (<0.05 to be stats-sig) '.format(p_value))
  # print('Power is {:.4f}'.format(power))
  print('Power is {:.1%} (80% is needded) '.format(power))
  print()
  # print('Delta (treatment - control): {:.3f}'.format(delta))
  print('Delta (treatment - control): {:.1%}'.format(delta))
  print('Confidence Interval of Delta : ({:.1%}, {:.1%})'.format(ci[0], ci[1]))
  # print('Confidence Interval of Delta', ci)
