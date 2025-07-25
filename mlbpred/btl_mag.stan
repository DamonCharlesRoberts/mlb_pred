// Ordered Bradley-Terry model with Home-field advantage.

data {
  int<lower=1> N; // Number of games.
  int<lower=1> J; // Number of teams.
  array[N, 2] int T; // Matrix of team ids. T[N, 1] = Away. T[N, 2] = Home.
  matrix[N, 2] S; // Matrix of scores. S[N, 1] = Away score; S[N, 2] = Home.
}

transformed data {
  // Compute who won.
  // Place in a vector where:
  // - Pos. diff indicates the away team won.
  // - Neg. diff indicates the home team won.
  array[N] int<lower=1, upper=7> y;
  for (n in 1:N) {
    real diff = S[n, 1] - S[n, 2];
    if (diff<=-5)
      y[n] = 1;
    else if (diff<=-2 && diff>=-4)
      y[n] = 2;
    else if (diff==-1)
      y[n] = 3;
    else if (diff==0)
      y[n] = 4;
    else if (diff==1)
      y[n] = 5;
    else if (diff>=2 && diff<=4)
      y[n] = 6;
    else if (diff>=5)
      y[n] = 7;
  }
  // Create a vector indicating each team.
  array[N] int away = to_array_1d(T[,1]);
  array[N] int home = to_array_1d(T[,2]);
}

parameters {
  vector[J] alpha; // The ability for each team.
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
  // alpha_away * 1 - alpha_home * 0 + gamma
  for (n in 1:N){
    y[n] ~ ordered_logistic(alpha[away[n]] - alpha[home[n]] + gamma, c);
  }
}

generated quantities {
  // PPC.
  array[N] int<lower=1, upper=7> y_rep;
  for (n in 1:N){
    y_rep[n] = ordered_logistic_rng(alpha[away[n]] - alpha[home[n]] + gamma, c);
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
