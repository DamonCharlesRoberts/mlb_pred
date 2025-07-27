using DataFrames
using DuckDB
using Turing
using StatsPlots

include("./mlbpred/Utils.jl")

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
            , teams.team_abbr
            , dense_rank() over(order by home_team) as home_team
            , dense_rank() over(order by away_team) as away_team
            , home_runs
            , away_runs
            , (case
                when home_runs > away_runs then 0
                else 1
            end) as home_win
        from a
            left join teams
                on a.home_team=teams.team_id
        where teams.season_id like '2025'
        """
    )
)

ids = Dict(Pair.(df.team_abbr, df.home_team))

# Define the models. 
@model function simple(x::Array, y::Array, d::Integer)
    α ~ filldist(truncated(Normal(0., 1.), 0., Inf), d)
    for i in 1:length(y)
        θ = log(α[x[i,1]]) - log(α[x[i,2]])
        y[i] ~ BernoulliLogit(θ)
    end
end

@model function home(x::Array, y::Array, d::Integer)
    α ~ filldist(trunctated(Normal(0., 1.), 0., Inf), d)
    γ ~ Normal(0., 1.)
    for i in 1:length(y)
        θ = γ + log(α[x[i,1]]) - log(α[x[i,2]])
        y[i] ~ BernoulliLogit(θ)
    end
end

mod = simple(Matrix(select(df, [:home_team, :away_team])), df.home_win, maximum(df.home_team))
fit = Turing.sample(mod, NUTS(), MCMCThreads(), 4_000, 4)
plt = plot_rank(fit, ids)
