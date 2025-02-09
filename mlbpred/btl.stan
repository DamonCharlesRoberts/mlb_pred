// Simple Bradley-Terry model.

data {
  int<lower=1> N; // Number of games.
  matrix[N, 2] T; // Matrix of team ids. T[N, 1] = Away. T[N, 2] = Home.
  matrix[N, 2] S; // Matrix of scores. S[N, 1] = Away score; S[N, 2] = Home.
}

transformed data {
  // Compute who won.
  // Place in a vector where:
  // - 1 indicates the away team won
  // - 0 indicates the home team won
  vector<lower=0, upper=1>[N] y;
  for (n in 1:N) {
    real diff = S[n, 1] - S[n, 2];
    if (diff < 0)
      y[n] = 1
    else
      y[n] = 0
  }
}

parameters {
  vector[2] alpha; // The ability for each team.
}

model {
  // Prior on a logged-odds scale of the ability for each team.
  alpha ~ normal(0, 1);
  // Compute the ability for each team given who won.
  // The ability for the away team is dependent on whether they
  // beat the home team.
  // If the away team won, then the logged odd would be
  // alpha_away * 1 - alpha_home * 0
  y ~ bernoulli_logit(alpha[T[,1]] - alpha[T[,2]])
}

generated quantities {
  // Now compute the ranking of each team based on who won.
  // Do it based on descending logged odds (ability).
  vector[2] rank; // Ranking of the teams.
  {
    vector[2] rank_index = sort_indices_desc(alpha);
    for (i in 1:2) {
      rank[rank_index[i]] = i;
    }
  }
}
