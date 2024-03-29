import mysql.connector
import db_operations
import yt_requests
import indicators
import subtitle_score
import time
import numpy as np
import argparse
from sklearn.preprocessing import StandardScaler
import pandas as pd

# Database options
db_username = "root"
db_pass = "x"
db_name = "test"
db_host = "127.0.0.1"
db_port = "3306"


def get_statistics(videos_df, api_key, time_interval=3600, iterations=48):
    """
    Collects samples of the statistics of the videos between set time intervals.
    The collected statistics are inserted into the db on every iteration.
    :param videos_df: dataframe with video ids
    :param api_key: the API key to use when making requests to YouTube
    :param time_interval: time between samples (in seconds)
    :param iterations: number of samples to collect
    :return: None
    """
    counter = 0
    while counter < iterations:
        print(counter, ' Getting statistics...')
        start = time.time()
        statistics_df = yt_requests.get_views_likes_dislikes(api_key=api_key, df_in1=videos_df)
        end = time.time()
        print("TIME: yt_requests.get_views_likes_dislikes in ", end - start, " seconds.")
        try:
            # Connect to mysql server
            cnx = db_operations.db_connect(db_username, db_pass, db_host, db_port, db_name)
        except mysql.connector.Error as err:
            print(err)
            print("Connection to db failed")
        else:
            # insert statistics DataFrame into db
            print("Inserting data in DB...")
            start = time.time()
            db_operations.insert_data(cnx, db_operations.insert_statistics_query, statistics_df)
            end = time.time()
            print("TIME: Inserted statistics in DB in ", end - start, " seconds.")
            # close the connection
            cnx.close()
            print(counter, 'Complete')
            counter += 1
        print('Waiting ', time_interval, ' seconds! ')
        time.sleep(time_interval)
    print('Finished!')


def db_initialization(api_key, file_path):
    """
    Initializes the program: Collects video information and statistics, sets up the db,
    inserts video and statistics into the db
    :param api_key: the API key to use when making requests to YouTube
    :param file_path: The system directory where downloaded subtitle files will be saved
    :return: None
    """
    print("initializing")
    # get videos data
    start = time.time()
    video_dataframe = yt_requests.get_videos(api_key, file_path)
    end = time.time()
    print("TIME: yt_requests.get_videos in ", end - start, " seconds.")
    try:
        # Connect to mysql server
        cnx = db_operations.db_connect(db_username, db_pass, db_host, db_port)
        # Create db and tables
        db_operations.db_setup(cnx, db_name, db_operations.tables())
    except mysql.connector.Error as err:
        print(err)
        print("db setup failed")
    else:
        print("db setup finished")
        # insert videos data
        start = time.time()
        db_operations.insert_data(cnx, db_operations.insert_videos_query, video_dataframe)
        end = time.time()
        print("TIME: Finished inserting videos in DB in ", end - start, " seconds.")
        # query data from videos table
        start = time.time()
        video_dataframe = db_operations.fetch_data(cnx, 'videos')
        end = time.time()
        print("TIME: Finished querying videos from DB in ", end - start, " seconds.")
        # close the connection
        cnx.close()
        print("Get statistics START")
        dt = 3600   # int(input('Specify sample frequency in seconds: '))
        it = 48     # int(input('Specify number of samples: '))
        get_statistics(video_dataframe[['id']], api_key, time_interval=dt, iterations=it)
        print("Get statistics COMPLETE")


def subtitle_filtering():
    """
    Calculates and adds a score to the video's captions, deletes videos with low scores from the db
    :return: None
    """
    # Connect to mysql server
    cnx = db_operations.db_connect(db_username, db_pass, db_host, db_port, db_name)
    subs_df = db_operations.fetch_data(cnx, 'videos')
    subs_scores = subtitle_score.subtitle_scoring(subs_df[['id', 'captions']])
    # insert scores in db (reorder columns because of update statement)
    print("Inserting Scores in db")
    db_operations.insert_data(cnx, db_operations.update_scores, subs_scores[['captions_score', 'id']])
    # drop columns where captions_score = NULL
    print("Filtering videos from db")
    db_operations.delete_where_captions_score_null(cnx)
    # close the connection
    cnx.close()


def indicators_analysis():
    """
    Calculates and saves in the db indicators for the videos in the db
    Plots views, likes and dislikes of top 3 and bottom 3 videos according to ci indicator
    Calculates and plots correlation matrix of indicators, video duration and subtitle score
    :return: None
    """
    # Connect to mysql server
    cnx = db_operations.db_connect(db_username, db_pass, db_host, db_port, db_name)
    # query data from statistics table
    video_info = db_operations.fetch_data(cnx, 'videos')
    latest_stats = db_operations.fetch_data(cnx, 'statistics', date_time='latest')
    earliest_stats = db_operations.fetch_data(cnx, 'statistics', date_time='earliest')
    # calculate indicators
    indicators_df = indicators.calc(earliest_stats, latest_stats, video_info[['id', 'captions']])
    # save indicators data to db
    print("Inserting Indicators in db")
    # drop unnecessary columns
    indicators_df.drop(columns=['d_total_views', 'd_likes', 'd_dislikes'], inplace=True)
    # replace infinity values with NaN (inf creates errors to the DB)
    # and reset video_id to be a column
    indicators_df = indicators_df.replace([np.inf, -np.inf], np.nan).reset_index()
    # insert indicators in db (change column order to match query)
    db_operations.insert_data(cnx,
                              db_operations.update_indicators,
                              indicators_df[['p', 'r', 'LPV', 'DPV', 'VPD', 'ci', 'video_id']])

    indicators_df.set_index('video_id', inplace=True)
    # Plot views, likes and dislikes of top 3 and bottom 3 videos according to ci indicator
    print("Plot views, likes and dislikes")
    # get ids of videos with the top 3 and bottom 3 ci values
    ci = indicators_df['ci'].sort_values()
    top3, bottom3 = ci.iloc[:3].index.tolist(), ci.iloc[-3:].index.tolist()
    for video_id in top3 + bottom3:
        video_stats = db_operations.fetch_data(cnx, 'statistics', v_id=video_id)
        indicators.plot_views_likes_dislikes(video_stats)

    # Calculate and plot correlation matrix of indicators, video duration and subtitle score
    video_info = video_info.drop(columns=['title', 'url', 'captions'])
    indicators.correlation(video_info.set_index('id'))

    # normalize views
    # create a scaler object
    std_scaler = StandardScaler()
    views = latest_stats.set_index('video_id')
    video_info.set_index('id', inplace=True)
    video_info['std_views'] = pd.DataFrame(std_scaler.fit_transform(views['view_count'].to_numpy().reshape(-1, 1)),
                                           columns=['view_count'], index=views.index)
    # video information shown in order of standardized views
    video_info.sort_values('std_views', ascending=False).to_csv("results.csv")

    # close the connection
    cnx.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('api_key',
                        help="the API key to use when making requests to YouTube")
    parser.add_argument('subs_dir',
                        help="the system directory where downloaded subtitle files will be saved")
    args = parser.parse_args()
    print("API key=", args.api_key, " Subtitle Directory=", args.subs_dir)
    # STEP 1
    db_initialization(args.api_key, args.subs_dir)
    # STEP 2
    subtitle_filtering()
    # STEP 3
    indicators_analysis()

