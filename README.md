# PySpark-Analysis-on-Amazon-Tweets

## DATA DESCRIPTION
This dataset consists of 400,000+ tweets on Twitter, where users had contacted (@AmazonHelp) to give a review on amazon's service. 
The dataset includes several columns of data that can be analyzed:
* id_str: string id
* tweet_created_at: the date the tweet was created
* user_screen_name: the screen name of the twitter user
* user_id_str: user string id
* user_statuses_count: amount each user has of tweets
* user_favourites_count: amount of favourites each user has
* user_protected: TRUE or FALSE values for whether twitter user has a private account
* user_listed_count: count of user listed amount
* user_following: whether twitter user was following the @AmazonHelp account
* user_description: Bio of Twitter User
* user_location: location of Twitter User
* user_verified: TRUE or FALSE values for whether twitter user is verified or not
* user_followers_count: amount of followers each user has
* user_friends_count: amount of friends each user has
* user_created_at: when the twitter user account was created
* tweet_language: what language the tweet is in
* text_: content of the tweet sent
* favorite_count: amount of favorites on the tweet
* favorited: TRUE or FALSE values whether the tweet had been favorited
* in_reply_to_screen_name: screen name of the account that was replied to
* in_reply_to_status_id_str: string id value of account that was replied to
* in_reply_to_user_id_str: string user id value of account that was replied to
* retweet_count: number of amount of times the tweet was retweeted
* retweeted: TRUE or FALSE values whether the tweet was retweeted
* text: content of the tweet sent

## Goals
**1.**
Finding the Twitter users that are active on twitter in the dataset. Only counting the users that have posted in the last 5 days. Additionally, saving their "user_screen_name" and "user_id_str" in a dataframe "daily_active_users"

**2.**
Conducting a sample A/B test on Twitter. The Experiment.txt file includes "user_id_str", which are users that are selected as potential experiment targets. Creating a dataframe "experiment_user" to document the selected users and determine if they are active users

**3.**
Using new revised experiment target list with the "final_experiment.csv" file. In this file, several users were removed and a new column "info" has been added that includes whether the user is a female (F) or male (M). Joing the dataframes from Task 1 & 2 and saving the result in a "final_experiment" dataframe

## RESULTS
Running the code on a 2021 M1 Pro, Python 3.9.12, and Spark version 3.2.1, the following code completed in 36.8 seconds. This shows the advantage of using Spark for huge datasets like this. I might re-create the above code to pandas and seeing how long it takes to run without a spark session as a test
