// Simple Bradley-Terry model.

data {
  int<lower=1> N; // Number of games.
  int<lower=1> J; // Number of teams.
  array[N, 2] int T; // Matrix of team ids. T[N, 1] = Away. T[N, 2] = Home.
  matrix[N, 2] S; // Matrix of scores. S[N, 1] = Away score; S[N, 2] = Home.
}

transformed data {
  // Compute who won.
  // Place in a vector where:
  // - 1 indicates the away team won
  // - 0 indicates the home team won
  array[N] int<lower=0, upper=1> y;
  for (n in 1:N) {
    real diff = S[n, 1] - S[n, 2];
    if (diff < 0)
      y[n] = 0;
    else
      y[n] = 1;
  }
  // Create a vector indicating each team.
  array[N] int away = to_array_1d(T[,1]);
  array[N] int home = to_array_1d(T[,2]);
}

parameters {
  vector[J] alpha; // The ability for each team.
}

model {
  // Prior on a logged-odds scale of the ability for each team.
  alpha ~ normal(0, 1);
  // Compute the ability for each team given who won.
  // The ability for the away team is dependent on whether they
  // beat the home team.
  // If the away team won, then the logged odd would be
  // alpha_away * 1 - alpha_home * 0
  y ~ bernoulli_logit(alpha[away] - alpha[home]);
}

generated quantities {
  // PPC
  array[N] int<lower=0, upper=1> y_rep;
  y_rep = bernoulli_logit_rng(alpha[away] - alpha[home]);
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
