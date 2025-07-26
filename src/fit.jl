using DataFrames
using DuckDB
using Turing

# Get the data.
con = DBInterface.connect(DuckDB.DB, "./data/twenty_five.db")
df = DataFrame(
    DBInterface.execute(
        con 
        , """
        with a as (
            select
                scores.game_id
                , schedule.home_team
                , scores.home_runs
                , schedule.away_team
                , scores.away_runs
            from scores
                left join schedule
                on scores.game_id=schedule.game_id
            where schedule.season_id like '2025'
        )
        select
            game_id
            , home_team
            , away_team
            , (case
                when home_runs > away_runs then 1
                else 0
            end) as home_win
        from a
        """
    )
)

team_idx = sortperm(unique(append!(copy(df.home_team), copy(df.away_team))))

# Define the models. 
@model function simple(x::Array, y::Array, d::Vector)
    α ~ filldist(truncated(Normal(0., 1.), 0., Inf), d)
    θ = 1. ./ 1. .+ exp.(-(log.(α[x[:,1]]) .- log.(α[x[:,2]])))
    y .~ Bernoulli.(θ)
end

mod = simple(Matrix(select(df, [:home_team, :away_team])), df.home_win, team_idx)
fit = Turing.sample(mod, NUTS(), MCMCThreads(), 1_000, 4)
