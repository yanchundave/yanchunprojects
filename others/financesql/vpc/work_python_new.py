base_build = datasets["Borrowing Base - Past 30 Days"].copy()
base_build['outstanding_principle'] = base_build['receivable_outstanding'] - base_build['tip_fee_receivable']
base_build['eligibal_receivable'] = \
  base_build['Gross Advances - 14 Days or Less Delinquent-Past-Due-Date'] - \
  base_build['Advances in Excess of Size 600'] - \
  base_build['Advances in Excess of Term Criteria'] - \
  base_build['additional_ineligible_advances']
base_build = pd.concat([base_build, base_build.agg(["sum"])], ignore_index=True)
base_build = base_build.round(2)

col_names =  ['Borrow Base', 'Big Money(>=$25)', 'Tiny Money (<$25)', 'Total']
display_table  = pd.DataFrame(columns = col_names)

report_date = base_build['Report Date'][0]
display_table.loc[0] = ['Report Date - ' + report_date, '','','']

display_table.loc[1] = [
  'Receivable Outstanding',"{:,}".format(base_build['receivable_outstanding'][0]),
  "{:,}".format(base_build['receivable_outstanding'][1]),
  "{:,}".format(base_build['receivable_outstanding'][2])
]
display_table.loc[2] = [
  'Less: Tips / Fee Receivable',"{:,}".format(base_build['tip_fee_receivable'][0]),
  "{:,}".format(base_build['tip_fee_receivable'][1]),
  "{:,}".format(base_build['tip_fee_receivable'][2])
  ]

# A: gross advance outstanding (principal only)

display_table.loc[3] = [
  'Gross Advance Outstanding (Principal Only)',"{:,}".format(round(base_build['outstanding_principle'][0],2)),
  "{:,}".format(round(base_build['outstanding_principle'][1],2)),
  "{:,}".format(round(base_build['outstanding_principle'][2],2))
  ]

# B: less: advances 15 or more DPD
display_table.loc[4] = [
  'Less: Advances 15 or more DPD',"{:,}".format(base_build['Gross Advances - 15 Days or More Delinquent-Past-Due-Date'][0]),
  "{:,}".format(base_build['Gross Advances - 15 Days or More Delinquent-Past-Due-Date'][1]),
  "{:,}".format(base_build['Gross Advances - 15 Days or More Delinquent-Past-Due-Date'][2])
  ]

# C: less: advances 14 or less DPD
# Advances - 14 Days or Less Delinquent-Past-Due-Date
display_table.loc[5] = [
  'Subtotal - Gross Advances 14 or Less DPD',"{:,}".format(base_build['Gross Advances - 14 Days or Less Delinquent-Past-Due-Date'][0]),
  "{:,}".format(base_build['Gross Advances - 14 Days or Less Delinquent-Past-Due-Date'][1]),
  "{:,}".format(base_build['Gross Advances - 14 Days or Less Delinquent-Past-Due-Date'][2])
  ]

# D: Less Advances in Excess of Size
display_table.loc[6] = [
  'Less: Advances in Excess of Size ($600)',"{:,}".format(base_build['Advances in Excess of Size 600'][0]),
  "{:,}".format(base_build['Advances in Excess of Size 600'][1]),
  "{:,}".format(base_build['Advances in Excess of Size 600'][2])
  ]

# E: Less Advances in Excess of Term Criteria
display_table.loc[7] = [
  'Less: Advances in Excess of Term Criteria (30 Days)',"{:,}".format(base_build['Advances in Excess of Term Criteria'][0]),
  "{:,}".format(base_build['Advances in Excess of Term Criteria'][1]),
  "{:,}".format(base_build['Advances in Excess of Term Criteria'][2])
  ]

# F: Additional Ineligible Receivables: Receivables from multi-advance takers and advances with modified payback date
display_table.loc[8] = [
  'Less: Additional Ineligible Receivables',"{:,}".format(base_build['additional_ineligible_advances'][0]),
  "{:,}".format(base_build['additional_ineligible_advances'][1]),
  "{:,}".format(base_build['additional_ineligible_advances'][2])
  ]

# new line
display_table.loc[9] = [
  'Gross Eligible Receivables',"{:,}".format(base_build['eligibal_receivable'][0]),
  "{:,}".format(base_build['eligibal_receivable'][1]),
  "{:,}".format(base_build['eligibal_receivable'][2])
  ]


display_table.loc[11] = [
  'Eligible Receivables',"{:,}".format(base_build['eligibal_receivable'][0]),
  "{:,}".format(base_build['eligibal_receivable'][1]),
  "{:,}".format(base_build['eligibal_receivable'][2])
  ]

total_eligible_receivables = base_build['eligibal_receivable'][2]
applicable_advance_rate_on_receivables = 0.80
if(total_eligible_receivables >= 51000000 and total_eligible_receivables < 75000000):
  applicable_advance_rate_on_receivables = 0.85
elif(total_eligible_receivables >= 75000000):
  applicable_advance_rate_on_receivables = 0.90

display_table.loc[12] = [
  'x Applicable Advance Rate on Receivables',applicable_advance_rate_on_receivables*100,
  applicable_advance_rate_on_receivables*100,
  applicable_advance_rate_on_receivables*100
  ]

advance_on_receivables_big_money = base_build['eligibal_receivable'][0] * applicable_advance_rate_on_receivables
advance_on_receivables_tiny_money = base_build['eligibal_receivable'][1] * applicable_advance_rate_on_receivables
total_advance_on_receivables = advance_on_receivables_big_money + advance_on_receivables_tiny_money
display_table.loc[13] = [
  'Advance On Receivables',"{:,}".format(round(advance_on_receivables_big_money,2)),
  "{:,}".format(round(advance_on_receivables_tiny_money,2)),
  "{:,}".format(round(total_advance_on_receivables,2))
  ]

display_table.loc[14] = [
  '% Total',"{:,}".format(round((advance_on_receivables_big_money/total_advance_on_receivables)*100,2)),
  "{:,}".format(round((advance_on_receivables_tiny_money/total_advance_on_receivables)*100,2)),
  '100%'
  ]


# Memo
display_table.loc[15] = ['Memo: Excess Concentration Limits','','','']

current_excess_concentration_limits_big_money = base_build['Current - Excess Concentration Limits'][0]
current_excess_concentration_limits_tiny_money = base_build['Current - Excess Concentration Limits'][1]
total_current_excess_concentration_limits = base_build['Current - Excess Concentration Limits'][2]

other_excess_concentration_limits_big_money = base_build['1-14 DPD - Excess Concentration Limits'][0]
other_excess_concentration_limits_tiny_money = base_build['1-14 DPD - Excess Concentration Limits'][1]
total_other_excess_concentration_limits = base_build['1-14 DPD - Excess Concentration Limits'][2]

# percentage on DPD between 1 and 14
total_percentage_big_money_DPD = (other_excess_concentration_limits_big_money/total_other_excess_concentration_limits) * 100
total_percentage_tiny_money_DPD = (other_excess_concentration_limits_tiny_money/total_other_excess_concentration_limits) * 100

total_eligible_receivables_big_money = current_excess_concentration_limits_big_money + other_excess_concentration_limits_big_money
total_eligible_receivables_tiny_money = current_excess_concentration_limits_tiny_money + other_excess_concentration_limits_tiny_money
total_eligible_receivables = total_eligible_receivables_big_money + total_eligible_receivables_tiny_money

display_table.loc[16] = [
  'Current',
  "{:,}".format(round(current_excess_concentration_limits_big_money,2)),
  "{:,}".format(round(current_excess_concentration_limits_tiny_money,2)),
  "{:,}".format(round(total_current_excess_concentration_limits,2))
  ]
display_table.loc[17] = [
  '1-6 DPD',
  "{:,}".format(round(other_excess_concentration_limits_big_money,2)),
  "{:,}".format(round(other_excess_concentration_limits_tiny_money,2)),
  "{:,}".format(round(total_other_excess_concentration_limits,2))
  ]
display_table.loc[18] = [
  'Total %',
  "{:,}".format(round(total_percentage_big_money_DPD,2)),
  "{:,}".format(round(total_percentage_tiny_money_DPD,2)),
  ''
  ]

display_table.loc[19] = [
  'Total Eligible Receivables',
  "{:,}".format(round(total_eligible_receivables_big_money,2)),
  "{:,}".format(round(total_eligible_receivables_tiny_money,2)),
  "{:,}".format(round(total_eligible_receivables,2))
  ]

### Eligible Receivables with Advances > 100 ###

display_table.loc[20] = [
  'Eligible Receivables with Advances > 100',
  "{:,}".format(base_build['Eligible Receivables with Advances > 100'][0]),
  "{:,}".format(base_build['Eligible Receivables with Advances > 100'][1]),
  "{:,}".format(base_build['Eligible Receivables with Advances > 100'][2])
]


# Delinquency and Cushion Logic -- Current

delinquency_of_eligible_receivables = (total_other_excess_concentration_limits/total_eligible_receivables) * 100
cushion_of_delinquency_of_eligible_receivables = (0.06 * total_eligible_receivables) /total_other_excess_concentration_limits - 1
formatted_cushion_of_delinquency_of_eligible_receivables = \
  -100 * cushion_of_delinquency_of_eligible_receivables if cushion_of_delinquency_of_eligible_receivables < 0 else cushion_of_delinquency_of_eligible_receivables *100

delinquency_of_eligible_receivables_excess_concentration_amount = -1 * cushion_of_delinquency_of_eligible_receivables * total_other_excess_concentration_limits \
  if cushion_of_delinquency_of_eligible_receivables < 0 else 0


display_table.loc[22] = [
  'Delinquent % of Eligible Receivables',
  '',
  '',
  "{:,}".format(round(delinquency_of_eligible_receivables,2))]

display_table.loc[23] = ['Threshold','','','6%']
display_table.loc[24] = [
  '% Cushion',
  '',
  '',
  round(cushion_of_delinquency_of_eligible_receivables * 100, 2)
  ]
display_table.loc[25] = [
  'Excess Concentration Amount',
  '',
  '',
  "{:,}".format(round(delinquency_of_eligible_receivables_excess_concentration_amount,2))
  ]

# Delinquency and Cushion Logic -- new

display_table.loc[26] = ['Memo: Excess Concentration Limits of term 15 to 30 days','','','']

total_concentration_limits_15_30_days = base_build["15-30 Payback - Excess Concentration Limits"][2]

display_table.loc[27] = [
  '1-14 Payback',"{:,}".format(base_build['1-14 Payback - Excess Concentration Limits'][0]),
  "{:,}".format(base_build['1-14 Payback - Excess Concentration Limits'][1]),
  "{:,}".format(base_build['1-14 Payback - Excess Concentration Limits'][2])
  ]
display_table.loc[28] = [
  '15-30 Payback',"{:,}".format(base_build['15-30 Payback - Excess Concentration Limits'][0]),
  "{:,}".format(base_build['15-30 Payback - Excess Concentration Limits'][1]),
  "{:,}".format(base_build['15-30 Payback - Excess Concentration Limits'][2])
  ]

big_money_total = base_build['1-14 Payback - Excess Concentration Limits'][0] + base_build['15-30 Payback - Excess Concentration Limits'][0]
tiny_money_total = base_build['1-14 Payback - Excess Concentration Limits'][1] + base_build['15-30 Payback - Excess Concentration Limits'][1]

display_table.loc[29] = [
  'Total %',
  "{:,}".format(round(big_money_total / (big_money_total + tiny_money_total) ,2)),
  "{:,}".format(round(tiny_money_total / (big_money_total + tiny_money_total),2)),
  ''
  ]

display_table.loc[30] = [
  'Total Eligible Receivables',"{:,}".format(big_money_total),
  "{:,}".format(tiny_money_total),
  "{:,}".format(big_money_total + tiny_money_total)
  ]

delinquency_of_eligible_receivables_15_30_days = (total_concentration_limits_15_30_days / total_eligible_receivables) * 100
cushion_of_delinquency_of_eligible_receivables_15_30_days = (0.25 * total_eligible_receivables) /total_concentration_limits_15_30_days -1

formatted_cushion_of_delinquency_of_eligible_receivables_15_30_days = \
  -100 * cushion_of_delinquency_of_eligible_receivables_15_30_days if cushion_of_delinquency_of_eligible_receivables_15_30_days < 0 \
  else 100 * cushion_of_delinquency_of_eligible_receivables_15_30_days

delinquency_of_eligible_receivables_excess_concentration_amount_15_30_days = \
  -1 * cushion_of_delinquency_of_eligible_receivables_15_30_days * total_concentration_limits_15_30_days if delinquency_of_eligible_receivables_15_30_days > 25 \
  else 0


display_table.loc[31] = [
  'Delinquent % of Eligible Receivables term 15 to 30 days',
  '',
  '',
  "{:,}".format(round(delinquency_of_eligible_receivables_15_30_days,2))]

display_table.loc[32] = ['Threshold','','','25%']
display_table.loc[33] = [
  '% Cushion',
  '',
  '',
  round(cushion_of_delinquency_of_eligible_receivables_15_30_days * 100, 2)
  ]
display_table.loc[34] = [
  'Excess Concentration Amount term 15 to 30 days',
  '',
  '',
  "{:,}".format(round(delinquency_of_eligible_receivables_excess_concentration_amount_15_30_days,2))
  ]


# Tiny money excess amount

tiny_of_total_eligible = (total_eligible_receivables_tiny_money/total_eligible_receivables) * 100

cushion_of_tiny_of_total_eligible = (0.30 * total_eligible_receivables)/total_eligible_receivables_tiny_money - 1

tiny_of_total_eligible_excess_concentration_amount = -1 * cushion_of_tiny_of_total_eligible * total_eligible_receivables_tiny_money \
  if tiny_of_total_eligible > 30 else 0

display_table.loc[35] = ['Memo: Excess Concentration Limits of tiny money','','','']
display_table.loc[36] = ['Big / Tiny % of Total Eligible','',"{:,}".format(round(tiny_of_total_eligible,2)),'']
display_table.loc[37] = ['Threshold ','','30%','']
display_table.loc[38] = ['% Cushion ','',round(cushion_of_tiny_of_total_eligible,2),'']
display_table.loc[39] = ['Excess Concentration Amount ','',"{:,}".format(round(tiny_of_total_eligible_excess_concentration_amount,2)),'']

display_table.loc[40] = ['Memo: Effective Blended Advance Rate','Eligible $','% Advance','']

if(total_eligible_receivables < 51000000):
  up_to_50M = total_eligible_receivables
else:
  up_to_50M = 0

if(total_eligible_receivables >= 51000000 and total_eligible_receivables < 75000000):
  from_50_to_75M = total_eligible_receivables
else:
  from_50_to_75M = 0

if(total_eligible_receivables >= 75000000):
  greater_than_75M = total_eligible_receivables
else:
  greater_than_75M = 0

total = up_to_50M + from_50_to_75M + greater_than_75M
display_table.loc[41] = [
  'Up to $50M',
  '',
  '80%',"{:,}".format(round(up_to_50M,2))
  ]
display_table.loc[42] = [
  '$50 - $75M Bucket',
  '',
  '85%',
  "{:,}".format(round(from_50_to_75M,2))
  ]
display_table.loc[43] = [
  'Greater than $75M Bucket',
  '',
  '90%',
  "{:,}".format(round(greater_than_75M,2))
  ]
display_table.loc[44] = [
  'Total',
  '',
  '',
  "{:,}".format(round(total,2)),applicable_advance_rate_on_receivables*100
  ]

display_table.set_index('Borrow Base', inplace=True)

display_table