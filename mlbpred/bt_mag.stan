// Ordered Bradley-Terry model with Home-field advantage.
data {
  int<lower=1> N; // Number of games.
  int<lower=1> J; // Number of teams.
  array[N, 2] int X; // Matrix of team ids.
  array[N] int<lower=1, upper=7> y; // Home team win and by how much?
}

parameters {
  vector<lower=0>[J] alpha; // The ability for each team.
  real gamma; // Intercept term to provide a home-field advantage.
  ordered[6] c; // Number of cutpoints.
}

model {
  // Prior on a logged-odds scale of the ability for each team.
  alpha ~ normal(0, 1);
  // Prior for home-field advantage.
  gamma ~ normal(0, 1);
  // Compute the ability for each team given who won.
  // The ability for the away team is dependent on whether they
  // beat the home team.
  // If the away team won, then the logged odd would be
  for (n in 1:N){
    y[n] ~ ordered_logistic(log(alpha[X[n,1]]) - log(alpha[X[n,2]]) + gamma, c);
  }
}

generated quantities {
  // PPC.
  array[N] int<lower=1, upper=7> y_rep;
  for (n in 1:N){
    y_rep[n] = ordered_logistic_rng(log(alpha[X[n,1]]) - log(alpha[X[n,2]]) + gamma, c);
  }
  // Now compute the ranking of each team based on who won.
  array[J] int rank; // Ranking of the teams.
  {
    // Get the ranking of each team in descending order of the alpha.
    array[J] int rank_index = sort_indices_desc(alpha);
    // For each team, apply the rank so that the median value for rank[i]
    // is the rank for team i.
    for (i in 1:J) {
      rank[rank_index[i]] = i;
    }
  }
}
