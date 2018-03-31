
### Installation

Run the following command to install the dependencies:
pip install -r requirements.txt

### Run

To scrape instagram profile:
    magic.py -type instagram -username <username> -session_file path_to_session_file 
	
Example:
    magic.py -type instagram -username instagram_user -session_file instagram_sessions.csv 

	
To scrape twitter profile:
    magic.py -type twitter -username <username> -session_file path_to_session_file
	
Example:
    magic.py -type twitter -username twitter_user -session_file twitter_sessions.csv
	

### Session File

Instagram:
1 column named session containg the session key of a logged in user.
Sessions

Twitter:
4 columns containg the api keys.
consumer_key,consumer_secret,access_key,access_secret
