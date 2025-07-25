using DataFrames
using DuckDB

# Get the data.
con = DBInterface.connect(DuckDB.DB, "./data/twenty_five.db")
df_1_pos = DataFrame(
    DBInterface.execute(
        con 
        , """
        select
            sample_id
            , wave
            , choice_1
            , choice_2
            , (
                case when preferred==choice_1 then 1
                when preferred==choice_2 then 0
                else null
                end
            ) as y
        from responses
        where wave like '1'
        and pos_battery==1;
        """
    )
) 
