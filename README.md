# College Football Stats

Author: Nick Ruby

This project is a work in progress prediction model for college football.

Currently, this project pulls data from the [CFBD](https://collegefootballdata.com) API using a modular ETL tool. The ETL is written to allow easy implementations of new data transfer flows from various sources into this application's database. 

I wrote this project in [Python](https://www.python.org/) due to it's advanced data processing tools and seamless compatibility with MongoDB. I chose [MongoDB](https://www.mongodb.com/) for it's evolving data schemas and support of unstructured data which will be useful as the project grows and pulls data from various sources.

## Planned Features

- Pull weekly game data from the previous week.
- Analyze teams' performances on a weekly bases to create predictions of upcoming matchups.
- Create a web UI to allow users to view game predictions and team statistics.
- Pull data while games are in progress to allow users to see realtime game updates.