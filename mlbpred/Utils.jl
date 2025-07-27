"""
    rank

Ranking the ability scores, α, for each object in each posterior draw.

Args:
    x (Chains): The result of sampling the turing.jl model.
    d (Int8): The number of objects.

Returns:

"""
function rank(x, d)
    # Create a matrix of samples for the α parameter.
    samples = MCMCChains.group(x, :α).value
    # Initialize an array of rankings.
    rank_arr = Array{Integer, 3}(undef, 1_000, length(ids), 4)
    # Rank the α for each sample.
    # This should produce an array with the ranking for each object -- in order.
    for c in 1:4
        for i in 1:1_000
            # Get the current sample for iteration i and chain j
            current_sample = samples[i, :, c]
            # Rank the options by sorting the α values in descending order (higher α means higher rank)
            ranked_indices = sortperm(current_sample, rev=true)  # Sort descending
            # Assign ranks to each option based on sorted order
            for rank_idx in 1:length(d)
                rank_arr[i, ranked_indices[rank_idx], c] = rank_idx
            end
        end
    end
    # Initialize a DataFrame.
    df = DataFrame()
    # Place the rankings into a DataFrame.
    for c in 1:4
        for (key, value) in ids
            temp_df = DataFrame(
                iter = repeat(1_000:-1:1, outer=1)
                , Rank = rank_arr[:, value, c]
                , Team = key
                , chain = c
            )
            # Append the temporary DataFrame to the main DataFrame
            append!(df, temp_df)
        end
    end
     
    # Return the result.
    return df
end

"""
    plot_rank

Rank the objects given the Turing chains. Then plot
them. Return the plot.
"""
function plot_rank(fit, n)
    # Compute the ranks.
    df_rank = rank(fit, n)
    # Plot the rankings
    # - Compute the median rank value and add to plot.
    df_grouped = groupby(df_rank, :Team)
    df_med = combine(df_grouped, :Rank => median => :MedRank)
    plt = @df df_med scatter(
        :Team, :MedRank
        , legend=false
        , color=:black
        , marker=:circle
        , markersize=6
        , xticks=:all
        , xrotation=90
    )
    @df df_rank violin!(
        plt, :Team, :Rank
        , legend=false
        , ylabel="Rank"
        , fill=:lightgray
        , alpha=0.6
        , xticks=:all
        , xrotation=90
    )
    @df df_med scatter!(
        plt, :Team, :MedRank
        , legend=false
        , color=:black
        , marker=:circle
        , markersize=6
        , xticks=:all
        , xrotation=90
    )
    return plt
end
