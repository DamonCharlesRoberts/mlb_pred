// Simple Bradley-Terry model with Home-field advantage.
data {
  int<lower=1> N; // Number of games.
  int<lower=1> J; // Number of teams.
  array[N, 2] int X; // Matrix of team ids.
  array[N] int<lower=0, upper=1> y; // Did the home team win?
}

parameters {
  vector<lower=0>[J] alpha; // The ability for each team.
  real gamma; // Intercept term to provide a home-field advantage.
}

model {
  // Prior on a logged-odds scale of the ability for each team.
  alpha ~ normal(0, 1);
  // Prior for home-field advantage.
  gamma ~ normal(0, 1);
  // Compute the ability for each team given who won.
  y ~ bernoulli_logit(log(alpha[X[,1]]) - log(alpha[X[,2]]) + gamma);
}

generated quantities {
  // PPC.
  array[N] int<lower=0, upper=1> y_rep;
  y_rep = bernoulli_logit_rng(alpha[X[,1]] - alpha[X[,2]] + gamma);
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
