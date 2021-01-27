# Youtube Cartoon Recommendation System
_Assignment for the Python course in MSc Big Data and Analytics 2020-2021_

The aim of this project is to utilize the tools of programming, databases and Natural Language Processing (NLP) to discover any underlying connections between Youtube’s view-based ordering algorithm and the emotional content of cartoon videos. Proof of a strong relation between the two could potentially result in the improvement of the company’s cost policies, regarding the projected advertisements on Youtube.

### Database Operations (db_operations.py)
The database operations module is responsible for providing the operations to the MySQL database connected to the system. 
Communication with the database is achieved using the MySQL Connector Python driver. 

### YouTube Requests (yt_requests.py)
The Youtube Request module’s primary goal is to execute the requests required in order to retrieve the necessary data from Youtube. For this purpose the Google Youtube APIv3 and the pytube.YouTube module have been used

### Subtitle Score (subtitle_score.py)
The subtitle_score module’s function is to rate the captions of each video according to the useful information they contain. For this operation a custom caption quality index has been implemented that works as follows: Each subtitle text line gets a score equal to its duration, or 0, depending if the text is enclosed inside brackets or parenthesis. After that the total score of all the lines gets summed up and divided by the total duration of all the captions, thus resulting in a score in the range of [0,1], that represents the percentage of useful information contained within each caption file

### Indicators (indicators.py)
The indicators module is responsible for the calculation of a number of indicators to better describe the collected data, as well as their presentation with graphs and a correlation matrix

### Main (main.py)
The main module is coordinating all of the other modules, providing the desired functionality to the system.  The API key to use when making requests to YouTube, and the system directory where downloaded subtitle files will be saved are passed into the main module through positional command line arguments.
