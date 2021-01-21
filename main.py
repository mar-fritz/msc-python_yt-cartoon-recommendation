import mysql.connector
import db_operations
import yt_requests
import indicators
import time

db_username = "root"
db_pass = "x"
db_name = "test"
db_host = "127.0.0.1"
db_port = "3306"


def get_statistics(df_in, api_key, time_interval=3600, iterations=48):
    counter = 0
    while counter < iterations:
        print(counter, ' Getting statistics...')
        statistics_df = yt_requests.get_views_likes_dislikes(api_key=api_key, df_in1=df_in)
        try:
            # Connect to mysql server
            cnx = db_operations.db_connect(db_username, db_pass, db_host, db_port, db_name)
        except mysql.connector.Error as err:
            print(err)
            print("Connection to db failed")
        else:
            # insert statistics DataFrame into db
            print("Inserting data in DB...")
            db_operations.insert_data(cnx, db_operations.insert_statistics_query, statistics_df)
            # close the connection
            cnx.close()
            print(counter, 'Complete')
            counter += 1
        print('Waiting ', time_interval, ' seconds! ')
        time.sleep(time_interval)
    print('Finished!')


def db_initialization(api_key, file_path):
    print("initializing")
    # get videos data
    video_dataframe = yt_requests.get_videos(api_key, file_path)
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
        db_operations.insert_data(cnx, db_operations.insert_videos_query, video_dataframe)
        # query data from videos table
        video_dataframe = db_operations.fetch_data(cnx, 'videos')
        # close the connection
        cnx.close()
        print("Get statistics START")
        dt = int(input('Specify sample frequency in seconds: '))
        it = int(input('Specify number of samples: '))
        get_statistics(video_dataframe, api_key, time_interval=dt, iterations=it)
        print("Get statistics COMPLETE")


def indicators_analysis():
    # Connect to mysql server
    cnx = db_operations.db_connect(db_username, db_pass, db_host, "3306", db_name)
    # query data from statistics table
    latest_stats = db_operations.fetch_data(cnx, 'statistics', date_time='latest')
    earliest_stats = db_operations.fetch_data(cnx, 'statistics', date_time='earliest')
    # calculate indicators
    indicators_df = indicators.calc(earliest_stats, latest_stats)
    # TODO: save indicators data to db
    # # save as csv to analyze later
    # indicators_df.to_csv("indicators.csv")

    # Plot views, likes and dislikes of top 3 and bottom 3 videos according to ci indicator
    # get ids of videos with the top 3 and bottom 3 ci values
    ci = indicators_df['ci'].sort_values()
    top3, bottom3 = ci.iloc[:3].index.tolist(), ci.iloc[-3:].index.tolist()
    for video_id in top3 + bottom3:
        video_stats = db_operations.fetch_data(cnx, 'statistics', v_id=video_id)
        indicators.plot_views_likes_dislikes(video_stats)

    # Calculate and plot correlation matrix of indicators and video duration
    video_durations = db_operations.fetch_data(cnx, 'videos').drop(columns=['title', 'url', 'captions'])
    indicators.correlation(indicators_df, video_durations)

    # close the connection
    cnx.close()


if __name__ == '__main__':
    # print("test stuff here")
    # STEP 1
    db_initialization('your-api-key-here', "your/path/here")
    # STEP 3
    indicators_analysis()
