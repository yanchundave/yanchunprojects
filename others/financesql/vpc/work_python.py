df = datasets["Borrowing Base - Past 30 Days"]

base_build = datasets["Borrowing Base - Past 30 Days"]

col_names =  ['Borrow Base', 'Big Money(>=$25)', 'Tiny Money (<$25)', 'Total']
display_table  = pd.DataFrame(columns = col_names)

report_date = base_build['Report Date'][0]
display_table.loc[0] = ['Report Date - ' + report_date, '','','']

# big money
#base_build['receivable_outstanding'][0]
# tiny money
#base_build['receivable_outstanding'][1]
# total
total_receivable_outstanding = float(base_build['receivable_outstanding'][0]) + float(base_build['receivable_outstanding'][1])

# total tip/fee
total_tip_fee_receivable = float(base_build['tip_fee_receivable'][0]) + float(base_build['tip_fee_receivable'][1])
display_table.loc[1] = ['Receivable Outstanding',"{:,}".format(round(base_build['receivable_outstanding'][0],2)),"{:,}".format(round(base_build['receivable_outstanding'][1],2)),"{:,}".format(round(total_receivable_outstanding,2))]
display_table.loc[2] = ['Less: Tips / Fee Receivable',"{:,}".format(round(base_build['tip_fee_receivable'][0],2)),"{:,}".format(round(base_build['tip_fee_receivable'][1],2)),"{:,}".format(round(total_tip_fee_receivable,2))]

# A: gross advance outstanding (principal only)
gross_advance_outstanding_big_money = float(base_build['receivable_outstanding'][0]) - float(base_build['tip_fee_receivable'][0])
gross_advance_outstanding_tiny_money = float(base_build['receivable_outstanding'][1]) - float(base_build['tip_fee_receivable'][1])
total_gross_advance_outstanding = gross_advance_outstanding_big_money + gross_advance_outstanding_tiny_money
display_table.loc[3] = ['Gross Advance Outstanding (Principal Only)',"{:,}".format(round(gross_advance_outstanding_big_money,2)),"{:,}".format(round(gross_advance_outstanding_tiny_money,2)),"{:,}".format(round(total_gross_advance_outstanding,2))]

# B: less: advances 7 or more DPD
# Advances - 7 Days or More Delinquent-Past-Due-Date
advances_DPD_7days_big_money = base_build['Gross Advances - 7 Days or More Delinquent-Past-Due-Date'][0]
advances_DPD_7days_tiny_money = base_build['Gross Advances - 7 Days or More Delinquent-Past-Due-Date'][1]
total_advances_7days_DPD = advances_DPD_7days_big_money + advances_DPD_7days_tiny_money
display_table.loc[4] = ['Less: Advances 7 or more DPD',"{:,}".format(round(advances_DPD_7days_big_money,2)),"{:,}".format(round(advances_DPD_7days_tiny_money,2)),"{:,}".format(round(total_advances_7days_DPD,2))]

# C: less: advances 6 or less DPD
# Advances - 6 Days or Less Delinquent-Past-Due-Date
advances_DPD_6days_big_money = base_build['Gross Advances - 6 Days or Less Delinquent-Past-Due-Date'][0]
advances_DPD_6days_tiny_money = base_build['Gross Advances - 6 Days or Less Delinquent-Past-Due-Date'][1]
total_advances_6days_DPD = advances_DPD_6days_big_money + advances_DPD_6days_tiny_money
display_table.loc[5] = ['Subtotal - Gross Advances 6 or Less DPD',"{:,}".format(round(advances_DPD_6days_big_money,2)),"{:,}".format(round(advances_DPD_6days_tiny_money,2)),"{:,}".format(round(total_advances_6days_DPD,2))]

# D: Less Advances in Excess of Size
excess_of_size_big_money = base_build['Advances in Excess of Size 500'][0]
excess_of_size_tiny_money = base_build['Advances in Excess of Size 500'][1]
total_excess_of_size = excess_of_size_big_money + excess_of_size_tiny_money
display_table.loc[6] = ['Less: Advances in Excess of Size ($500)',"{:,}".format(round(excess_of_size_big_money,2)),"{:,}".format(round(excess_of_size_tiny_money,2)),"{:,}".format(round(total_excess_of_size,2))]

# E: Less Advances in Excess of Term Criteria
excess_of_term_big_money = base_build['Advances in Excess of Term Criteria'][0]
excess_of_term_tiny_money = base_build['Advances in Excess of Term Criteria'][1]
total_excess_of_term = excess_of_term_big_money + excess_of_term_tiny_money
display_table.loc[7] = ['Less: Advances in Excess of Term Criteria (14 Days)',"{:,}".format(round(excess_of_term_big_money,2)),"{:,}".format(round(excess_of_term_tiny_money,2)),"{:,}".format(round(total_excess_of_term,2))]

# F: Receivables based in Pennyslvania
pennyslvania_big_money = base_build['pa_receivable_outstanding_minus_tip_fee'][0] - (base_build['pa_users_terms_criteria'][0] + base_build['pa_users_advances_excess_of_size_500'][0] + base_build['pa_users_7_dpd'][0])
pennyslvania_tiny_money = base_build['pa_receivable_outstanding_minus_tip_fee'][1] - (base_build['pa_users_terms_criteria'][1] + base_build['pa_users_advances_excess_of_size_500'][1] + base_build['pa_users_7_dpd'][1])
total_pennyslvania = pennyslvania_big_money + pennyslvania_tiny_money
display_table.loc[8] = ['Less: Advances in Pennyslvania',"{:,}".format(round(pennyslvania_big_money,2)),"{:,}".format(round(pennyslvania_tiny_money,2)),"{:,}".format(round(total_pennyslvania,2))]

# G: Additional Ineligible Receivables: Receivables from multi-advance takers and advances with modified payback date
ineligible_receivables_big_money = base_build['additional_ineligible_advances'][0]
ineligible_receivables_tiny_money = base_build['additional_ineligible_advances'][1]
total_ineligible_receivables = ineligible_receivables_big_money + ineligible_receivables_tiny_money
display_table.loc[9] = ['Less: Additional Ineligible Receivables',"{:,}".format(round(ineligible_receivables_big_money,2)),"{:,}".format(round(ineligible_receivables_tiny_money,2)),"{:,}".format(round(total_ineligible_receivables,2))]

eligible_receivables_big_money = advances_DPD_6days_big_money - excess_of_term_big_money - excess_of_size_big_money - pennyslvania_big_money - ineligible_receivables_big_money
eligible_receivables_tiny_money = advances_DPD_6days_tiny_money - excess_of_term_tiny_money - excess_of_size_tiny_money - pennyslvania_tiny_money - ineligible_receivables_tiny_money

# new line
gross_eligible_receivables_big_money = eligible_receivables_big_money
gross_eligible_receivables_tiny_money = eligible_receivables_tiny_money
total_gross_eligible_receivables = gross_eligible_receivables_big_money + gross_eligible_receivables_tiny_money
display_table.loc[10] = ['Gross Eligible Receivables',"{:,}".format(round(gross_eligible_receivables_big_money,2)),"{:,}".format(round(gross_eligible_receivables_tiny_money,2)),"{:,}".format(round(total_gross_eligible_receivables,2))]

# Less: Excess Concentration Amounts

current_excess_concentration_limits_big_money = base_build['Current - Excess Concentration Limits'][0]
current_excess_concentration_limits_tiny_money = base_build['Current - Excess Concentration Limits'][1]
total_current_excess_concentration_limits = current_excess_concentration_limits_big_money + current_excess_concentration_limits_tiny_money

other_excess_concentration_limits_big_money = base_build['1-6 DPD - Excess Concentration Limits'][0]
other_excess_concentration_limits_tiny_money = base_build['1-6 DPD - Excess Concentration Limits'][1]
total_other_excess_concentration_limits = other_excess_concentration_limits_big_money + other_excess_concentration_limits_tiny_money

# percentage on DPD between 1 and 6
total_percentage_big_money_DPD = (other_excess_concentration_limits_big_money/total_other_excess_concentration_limits) * 100
total_percentage_tiny_money_DPD = (other_excess_concentration_limits_tiny_money/total_other_excess_concentration_limits) * 100

total_eligible_receivables_big_money = current_excess_concentration_limits_big_money + other_excess_concentration_limits_big_money
total_eligible_receivables_tiny_money = current_excess_concentration_limits_tiny_money + other_excess_concentration_limits_tiny_money
total_other_eligible_receivables = other_excess_concentration_limits_big_money + other_excess_concentration_limits_tiny_money
total_eligible_receivables = total_eligible_receivables_big_money + total_eligible_receivables_tiny_money

### Eligible Receivables with Advances > 100 ###
eligible_receivables_with_advances_greater_than_100_big_money = base_build['Eligible Receivables with Advances > 100'][0]
eligible_receivables_with_advances_greater_than_100_tiny_money = base_build['Eligible Receivables with Advances > 100'][1]
total_eligible_receivables_with_advances_greater_than_100 = eligible_receivables_with_advances_greater_than_100_big_money + eligible_receivables_with_advances_greater_than_100_tiny_money

delinquency_of_eligible_receivables = (total_other_excess_concentration_limits/total_eligible_receivables) * 100
cushion_of_delinquency_of_eligible_receivables = ((12/((total_other_excess_concentration_limits/total_eligible_receivables)*100))-1)
if(cushion_of_delinquency_of_eligible_receivables < 0):
  formatted_cushion_of_delinquency_of_eligible_receivables = (cushion_of_delinquency_of_eligible_receivables*-1)*100
else:
  formatted_cushion_of_delinquency_of_eligible_receivables = cushion_of_delinquency_of_eligible_receivables * 100

cushion_of_delinquency_of_eligible_receivables = cushion_of_delinquency_of_eligible_receivables * 100

if(delinquency_of_eligible_receivables > 12):
  delinquency_of_eligible_receivables_excess_concentration_amount = formatted_cushion_of_delinquency_of_eligible_receivables/100 * total_other_excess_concentration_limits
else:
  delinquency_of_eligible_receivables_excess_concentration_amount = 0

tiny_of_total_eligible = (total_eligible_receivables_tiny_money/total_eligible_receivables) * 100
cushion_of_tiny_of_total_eligible = ((30/((total_eligible_receivables_tiny_money/total_eligible_receivables)*100))-1)*100
if(tiny_of_total_eligible > 30):
  tiny_of_total_eligible_excess_concentration_amount = cushion_of_tiny_of_total_eligible/100 * total_eligible_receivables_tiny_money
else:
  tiny_of_total_eligible_excess_concentration_amount = 0

# Advances > $100
advances_greater_than_100 = (total_eligible_receivables_with_advances_greater_than_100/total_eligible_receivables) * 100
if total_eligible_receivables_with_advances_greater_than_100 > 0 :
  cushion_advances_greater_than_100 = ((20 / ((total_eligible_receivables_with_advances_greater_than_100 / total_eligible_receivables) * 100)) - 1) * 100
else:
  cushion_advances_greater_than_100 = 0
if(advances_greater_than_100 > 20):
  advances_greater_than_100_excess_concentration_amount = cushion_advances_greater_than_100/100 * total_eligible_receivables
else:
  advances_greater_than_100_excess_concentration_amount = 0
total_excess_concentration_amount = delinquency_of_eligible_receivables_excess_concentration_amount+ tiny_of_total_eligible_excess_concentration_amount + advances_greater_than_100_excess_concentration_amount

big_money_excess_amount = (delinquency_of_eligible_receivables_excess_concentration_amount *  total_percentage_big_money_DPD/100) + advances_greater_than_100_excess_concentration_amount
tiny_money_excess_amount = (delinquency_of_eligible_receivables_excess_concentration_amount *  total_percentage_tiny_money_DPD/100) + tiny_of_total_eligible_excess_concentration_amount
#tiny_of_total_eligible_excess_concentration_amount
#display_table.loc[10] = ['Less: Excess Concentration Amounts',"{:,}".format(round(big_money_excess_amount,2)),"{:,}".format(round(tiny_money_excess_amount,2)),"{:,}".format(round(total_excess_concentration_amount,2))]

#total_eligible_receivables = eligible_receivables_big_money + eligible_receivables_tiny_money - total_excess_concentration_amount
total_eligible_receivables = eligible_receivables_big_money + eligible_receivables_tiny_money
#display_table.loc[11] = ['Eligible Receivables',"{:,}".format(round(eligible_receivables_big_money - big_money_excess_amount,2)),"{:,}".format(round(eligible_receivables_tiny_money-tiny_money_excess_amount,2)),"{:,}".format(round(total_eligible_receivables,2))]
display_table.loc[11] = ['Eligible Receivables',"{:,}".format(round(eligible_receivables_big_money, 2)),"{:,}".format(round(eligible_receivables_tiny_money, 2)),"{:,}".format(round(total_eligible_receivables,2))]
#display_table.loc[12] = ['x Applicable Advance Rate on Receivables','80.0%','80.0%','80.0%']

#$50M (80% advance rate), $51-$75M (85% advance rate) and $75M+ (90% advance rate
applicable_advance_rate_on_receivables = 0.80
if(total_eligible_receivables >= 51000000 and total_eligible_receivables < 75000000):
  applicable_advance_rate_on_receivables = 0.85
elif(total_eligible_receivables >= 75000000):
  applicable_advance_rate_on_receivables = 0.90

display_table.loc[12] = ['x Applicable Advance Rate on Receivables',applicable_advance_rate_on_receivables*100,applicable_advance_rate_on_receivables*100,applicable_advance_rate_on_receivables*100]

multiplier = 1 - applicable_advance_rate_on_receivables

borrowing_base_deduction_big_money = eligible_receivables_big_money * multiplier
borrowing_base_deduction_tiny_money = eligible_receivables_tiny_money * multiplier
total_borrowing_base_deduction = borrowing_base_deduction_big_money + borrowing_base_deduction_tiny_money

## removed - advance_on_receivables_big_money = eligible_receivables_big_money - borrowing_base_deduction_big_money
## removed - advance_on_receivables_tiny_money = eligible_receivables_tiny_money - borrowing_base_deduction_tiny_money
#advance_on_receivables_big_money = (eligible_receivables_big_money - big_money_excess_amount) * applicable_advance_rate_on_receivables
#advance_on_receivables_tiny_money = (eligible_receivables_tiny_money - tiny_money_excess_amount) * applicable_advance_rate_on_receivables
advance_on_receivables_big_money = (eligible_receivables_big_money) * applicable_advance_rate_on_receivables
advance_on_receivables_tiny_money = (eligible_receivables_tiny_money) * applicable_advance_rate_on_receivables
total_advance_on_receivables = advance_on_receivables_big_money + advance_on_receivables_tiny_money
display_table.loc[13] = ['Advance On Receivables',"{:,}".format(round(advance_on_receivables_big_money,2)),"{:,}".format(round(advance_on_receivables_tiny_money,2)),"{:,}".format(round(total_advance_on_receivables,2))]

display_table.loc[14] = ['% Total',"{:,}".format(round((advance_on_receivables_big_money/total_advance_on_receivables)*100,2)),"{:,}".format(round((advance_on_receivables_tiny_money/total_advance_on_receivables)*100,2)),'100%']

display_table.loc[15] = ['Memo: Excess Concentration Limits','','','']
display_table.loc[16] = ['Current',"{:,}".format(round(current_excess_concentration_limits_big_money,2)),"{:,}".format(round(current_excess_concentration_limits_tiny_money,2)),"{:,}".format(round(total_current_excess_concentration_limits,2))]
display_table.loc[17] = ['1-6 DPD',"{:,}".format(round(other_excess_concentration_limits_big_money,2)),"{:,}".format(round(other_excess_concentration_limits_tiny_money,2)),"{:,}".format(round(total_other_excess_concentration_limits,2))]
display_table.loc[18] = ['Total %',"{:,}".format(round(total_percentage_big_money_DPD,2)),"{:,}".format(round(total_percentage_tiny_money_DPD,2)),'']

#total_eligible_receivables = total_eligible_receivables_big_money + total_eligible_receivables_tiny_money
#display_table.loc[19] = ['Total Eligible Receivables',"{:,}".format(round(total_eligible_receivables_big_money,2)),"{:,}".format(round(total_eligible_receivables_tiny_money,2)),"{:,}".format(round(total_eligible_receivables,2))]
total_eligible_receivables = eligible_receivables_big_money + eligible_receivables_tiny_money
display_table.loc[19] = ['Total Eligible Receivables',"{:,}".format(round(eligible_receivables_big_money,2)),"{:,}".format(round(eligible_receivables_tiny_money,2)),"{:,}".format(round(total_eligible_receivables,2))]

### Eligible Receivables with Advances > 100 ###

display_table.loc[20] = ['Eligible Receivables with Advances > 100',"{:,}".format(round(eligible_receivables_with_advances_greater_than_100_big_money,2)),"{:,}".format(round(eligible_receivables_with_advances_greater_than_100_tiny_money,2)),"{:,}".format(round(total_eligible_receivables_with_advances_greater_than_100,2))]
display_table.loc[21] = ['Delinquent % of Eligible Receivables','','',"{:,}".format(round(delinquency_of_eligible_receivables,2))]
display_table.loc[22] = ['Threshold','','','12%']
display_table.loc[23] = ['% Cushion','','',round(cushion_of_delinquency_of_eligible_receivables,2)]
display_table.loc[24] = ['Excess Concentration Amount','','',"{:,}".format(round(delinquency_of_eligible_receivables_excess_concentration_amount,2))]

display_table.loc[25] = ['Big / Tiny % of Total Eligible','',"{:,}".format(round(tiny_of_total_eligible,2)),'']
display_table.loc[26] = ['Threshold ','','30%','']
display_table.loc[27] = ['% Cushion ','',round(cushion_of_tiny_of_total_eligible,2),'']
display_table.loc[28] = ['Excess Concentration Amount ','',"{:,}".format(round(tiny_of_total_eligible_excess_concentration_amount,2)),'']


# Advances > $100
#advances_greater_than_100 = (total_eligible_receivables_with_advances_greater_than_100/total_eligible_receivables) * 100
#cushion_advances_greater_than_100 = ((30/((total_eligible_receivables_with_advances_greater_than_100/total_eligible_receivables)*100))-1)*100
#display_table.loc[29] = ['% of Advances > $100','','',"{:,}".format(round(advances_greater_than_100,2))]
#display_table.loc[30] = ['Threshold  ','','','20%']
#display_table.loc[31] = ['% Cushion  ','','',round(cushion_advances_greater_than_100,2)]
#display_table.loc[32] = ['Excess Concentration Amount  ','',"{:,}".format(round(advances_greater_than_100_excess_concentration_amount,2)),'']

display_table.loc[34] = ['Memo: Effective Blended Advance Rate','Eligible $','% Advance','']

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
display_table.loc[35] = ['Up to $50M',"{:,}".format(round(up_to_50M,2)),'80%','']
display_table.loc[36] = ['$50 - $75M Bucket',"{:,}".format(round(from_50_to_75M,2)),'85%','']
display_table.loc[37] = ['Greater than $75M Bucket',"{:,}".format(round(greater_than_75M,2)),'90%','']
display_table.loc[38] = ['Total',"{:,}".format(round(total,2)),applicable_advance_rate_on_receivables*100,'']

display_table.set_index('Borrow Base', inplace=True)
display_table

advances = datasets["Eligible Receivables Download Data"]
mode.export_csv(advances)

borrowing = datasets["Borrowing Base CSV"]
#print (borrowing.head(5))
#mode.export_csv(borrowing)

base_build = datasets["Borrowing Base CSV"]

col_names =  ['Borrow Base', 'Big Money', 'Tiny Money', 'Total']

report_date = base_build['Report Date']


#base_build['total_receivable_outstanding'] = base_build['receivable_outstanding'].astype(float) + base_build['receivable_outstanding'].astype(float)

# total tip/fee
#base_build['total_tip_fee_receivable'] = base_build['tip_fee_receivable'].astype(float) + base_build['tip_fee_receivable'].astype(float)

# A: gross advance outstanding (principal only)
base_build['gross_advance_outstanding'] = round(base_build['receivable_outstanding'].astype(float) - base_build['tip_fee_receivable'].astype(float), 2)

# B: less: advances 7 or more DPD
# Advances - 7 Days or More Delinquent-Past-Due-Date
#base_build['advances_DPD_7days'] = base_build['Gross Advances - 7 Days or More Delinquent-Past-Due-Date'].astype(float)

# C: less: advances 6 or less DPD
# Advances - 6 Days or Less Delinquent-Past-Due-Date
#base_build['advances_DPD_6days'] = base_build['Gross Advances - 6 Days or Less Delinquent-Past-Due-Date'].astype(float)

# D: Less Advances in Excess of Size
#base_build['Advances in Excess of Size ($500)'] = base_build['Advances in Excess of Size 500'].astype(float)

# E: Less Advances in Excess of Term Criteria
#base_build['Advances in Excess of Term Criteria (14 Days)'] = base_build['Advances in Excess of Term Criteria'].astype(float)

# F: Receivables based in Pennyslvania
base_build['Advances in Pennyslvania'] = base_build['pa_receivable_outstanding_minus_tip_fee'].astype(float) - (base_build['pa_users_terms_criteria'].astype(float) + base_build['pa_users_advances_excess_of_size_500'].astype(float) + base_build['pa_users_7_dpd'].astype(float))

#base_build.head(5)
mode.export_csv(base_build)


