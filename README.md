# java-map-reduce
Using Twitter dataset to count and group number of followers in java.
The dataset represents the follower graph that contains links between tweeting users in the form: user_id, follower_id (where user_id is the id of a user and follower_id is the id of the follower). For example:
12,13 ;
12,14 ;
12,15 ;
16,17 ;
Here, users 13, 14 and 15 are followers of user 12, while user 17 is a follower of user 16. The small dataset is available in project 1 folder(10,000 lines of the complete dataset) is available in small-twitter.csv inside project1.

First, for each twitter user, you count the number of users she follows. Then, you group the users by their number of the users they follow and for each group you count how many users belong to this group. That is, the result will have lines such as:

10 30
which says that there are 30 users who follow 10 users.

The code demonstrates how to use and chain 2 map reduce jobs with a simple example.

The file output/part-r-00000 must contain the same results as in file small-solution.txt. (The file temp/part-r-00000 contains the temporary results from the first map-reduce job).
