# Data Definition

## Tweet info
* tweet_id
* user_id
## Geographic information
* latitude
* longitude
* state
## Legal time info
* legal_datetime
  legal_year  
  legal_month  
  legal_day  
  legal_hour  
  legal_minute  
  legal_second

  Our best guess for what time it was when the user made their tweet.
* legal_day_of_week

  0=Monday and 6=Sunday
* legal_time_offset_ci_lower, legal_time_offset_ci_point, legal_time_offset_ci_upper

  Variables representing a distribution of possible UTC offsets. The mean of this distribution is the `point` estimate. The lower 95% quantile is `lower`. The upper 95% quantile is `upper`.
* days_since_transition

  Variable representing the amount of time since/until a DST transition.

  Negative values are before a transition. A value of zero indicates the tweet was made exactly at the time that DST changed over. Positive values are after a transition.

  For tweets in timezones which do not experience DST, this is calculated as if the timezone did experience DST.
* is_dst

  Indicates if a tweet is made during DST. Note that a tweet is considered made during DST if any part of the United States is experiencing DST. If in a timezone where DST is not observed, such as Arizona time, and the rest of the US is experiencing DST, this will still be 1.

  Possible values:

   * 0 = not made during DST
   * 1 = made during DST  
* timezone_experiences_dst

  Indicates if a tweet comes from a timezone which experiences DST, sometime in the past year.
## Sentiment

These are sentiment scores from each sentiment regressor. Each score is standardized (z-scored.)

* score_afinn
* score_bert
* score_vader

## Treatment strength

These are indicator variable representing whether a tweet was issued within 1, 2, 3, or 4 weeks from a DST transition. They are 0 if the tweet is outside the period, and 1 if it is inside. This includes tweets from both before and after the transition.

* within_1wk_transition
* within_2wk_transition
* within_3wk_transition
* within_4wk_transition

## Spring/Fall indicator
* spring_fall_indicator

  Possible values:

   * S: Indicates a value in the spring months Febuary, March, April, or May.
   * F: Indicates a value in the fall months October, November, or December.
   * U: Otherwise.
