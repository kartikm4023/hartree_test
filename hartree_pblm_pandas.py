import pandas as pd

# Read Data
df1 = pd.read_csv('./data/dataset1.csv')
df2 = pd.read_csv('./data/dataset2.csv')

# Merge dataframes
df = pd.merge(df1, df2, how='left', on='counter_party')

# Generate the results dataframe
all_group = df.groupby(['legal_entity', 'counter_party', 'tier']).agg(
    max_rating=pd.NamedAgg(column='rating', aggfunc='max'),
    sum_value_ARAP=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status']=='ARAP'].sum()),
    sum_value_ACCR=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status']=='ACCR'].sum())
).reset_index()


# Adding total for each legal entity
total_legal_entity = df.groupby(['legal_entity']).agg(
    max_rating=pd.NamedAgg(column='rating', aggfunc='max'),
    sum_value_ARAP=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status']=='ARAP'].sum()),
    sum_value_ACCR=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status']=='ACCR'].sum())
).reset_index()

total_legal_entity['tier'] = 'Total'
total_legal_entity['counter_party'] = 'Total'

# Adding Total for each counterparty
total_counter_party = df.groupby(['counter_party']).agg(
   max_rating=pd.NamedAgg(column='rating', aggfunc='max'),
   sum_value_ARAP=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status'] == 'ARAP'].sum()),
   sum_value_ACCR=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status'] == 'ACCR'].sum())
).reset_index()
total_counter_party['legal_entity'] = 'Total'
total_counter_party['tier'] = 'Total'

# Adding Total for each tier
total_tier = df.groupby(['tier']).agg(
   max_rating=pd.NamedAgg(column='rating', aggfunc='max'),
   sum_value_ARAP=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status'] == 'ARAP'].sum()),
   sum_value_ACCR=pd.NamedAgg(column='value', aggfunc=lambda x: x[df['status'] == 'ACCR'].sum())
).reset_index()
total_tier['legal_entity'] = 'Total'
total_tier['counter_party'] = 'Total'

# Concating all to create final output
output = pd.concat([all_group, total_legal_entity, total_counter_party, total_tier])

# Save the results to a CSV file
output.to_csv('./data/pandas_result.csv', index=False)