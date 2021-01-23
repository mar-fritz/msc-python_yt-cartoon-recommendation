import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
import seaborn as sns
import sentiment_analysis
# https://matplotlib.org/3.3.3/api/_as_gen/matplotlib.axes.Axes.ticklabel_format.html
# https://matplotlib.org/3.1.1/api/matplotlib_configuration_api.html#matplotlib.RcParams
matplotlib.rcParams['axes.formatter.useoffset'] = False



def calc(earliest_stats, latest_stats, captions):
    """
    Creates pandas DataFrame with indicator values for all videos
    :param captions: DataFrame of captions
    :param earliest_stats: DataFrame of first stats of videos collected
    :param latest_stats: DataFrame of last stats of videos collected
    :return: DataFrame with indicator values
    """
    # construct indicators DataFrame
    earliest_stats = earliest_stats.set_index('video_id').drop('date_time', axis=1)
    latest_stats = latest_stats.set_index('video_id').drop('date_time', axis=1)
    indicators_df = latest_stats - earliest_stats
    indicators_df.rename(columns={"view_count": "d_total_views",
                                  "like_count": "d_likes",
                                  "dislike_count": "d_dislikes"}, inplace=True)
    # compute indicators
    # p = delta(likes) / delta(dislikes)
    # indicates if users have been liking or disliking the video
    indicators_df['p'] = indicators_df['d_likes'].div(indicators_df['d_dislikes'])
    # r
    indicators_df['r'] = (183 * indicators_df['d_total_views']) / latest_stats['view_count']
    # LPV number of likes weighted by the number of views in the data
    indicators_df['LPV'] = indicators_df['d_likes'].div(indicators_df['d_total_views'])
    # DPV number of dislikes weighted by the number of views in the data
    indicators_df['DPV'] = indicators_df['d_dislikes'].div(indicators_df['d_total_views'])
    # VPD = delta(views)/2  (daily views for the 2 days of data)
    indicators_df['VPD'] = indicators_df['d_total_views'] / 2
    # ci
    # calculate CI indicator
    sentiment = sentiment_analysis.analyze(captions)
    sentiment.set_index('id', inplace=True)
    indicators_df['ci'] = (sentiment['bayes_res'] + sentiment['pattern_res'])/2

    return indicators_df


def plot_views_likes_dislikes(statistics_df):
    """
    Plots views, likes, and dislikes of video in one plot, setting the id of the video as title.
    :param statistics_df: statistics DataFrame of a specific video
    :return: None
    """
    statistics_df.set_index('date_time', inplace=True)
    # print(statistics_df)
    statistics_df.plot(subplots=True, title=statistics_df['video_id'].iloc[0])
    plt.show()


def correlation(data):
    """
    Calculates and plots correlation matrix of video indicators, durations & subtitle scores
    :param data: DataFrame containing the indicators values of videos, video durations & subtitle scores
    :return: None
    """

    # replace infinity values with NaN (inf creates errors to the scaler)
    data = data.replace([np.inf, -np.inf], np.nan)

    # create a scaler object
    std_scaler = StandardScaler()
    # fit and transform the data
    df_std = pd.DataFrame(std_scaler.fit_transform(data), columns=data.columns, index=data.index)

    # create boolean mask
    triangl = np.tril(np.ones_like(df_std.corr(method='pearson'), dtype=bool))
    # calculate the correlation matrix, take absolute values, and apply the mask
    df_corr = df_std.corr(method='pearson').abs().mask(triangl)

    sns.heatmap(df_corr, annot=True, cmap='PuBu')
    plt.show()
