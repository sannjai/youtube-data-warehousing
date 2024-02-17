from googleapiclient.discovery import build
import pymongo
import pandas as pd
import json
from datetime import datetime
import streamlit as st
import mysql.connector
import pandas as pd
from pymongo import MongoClient

def api_connect():
  Api_id='AIzaSyCI4MrjINrl7DUqqX378PhlEO6Dr1t7Zuo'
  api_service_name='youtube'
  api_version='v3'
  youtube=build(api_service_name,api_version,developerKey=Api_id)
  return youtube

yt=  api_connect()

import requests


#GET CHANNEL DETAILS


def get_channel_info(channel_id):

  request = yt.channels().list(part="snippet,contentDetails,statistics",
        id=channel_id
    )
  response = request.execute()

  for i in response["items"]:
    data=dict(Channel_name=i['snippet']['title'],
              Channel_id=i["id"],
              Subscribers=i['statistics']['subscriberCount'],
              Views=i['statistics']['viewCount'],
              Total_videos=i['statistics']['videoCount'],
              Description=i['snippet']['description'],
              Playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
              )
  return data

#GET VIDEO ID DETAILS

def get_video_ids(channel_id):
  response=yt.channels().list(part='contentDetails',
          id=channel_id
      ).execute()
  Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  video_ids=[]

  next_Page_Token=None

  while True:
    response1=yt.playlistItems().list(part='snippet',
                                      playlistId=Playlist_id,
                                      maxResults=50,
                                      pageToken=next_Page_Token).execute()
    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

    next_Page_Token=response1.get('nextPageToken')

    if next_Page_Token is None:
      break
  return video_ids

#GET VIDEO DETAILS

def get_video_details(video_ids):
  video_data=[]
  for video_id in video_ids:

    request=yt.videos().list(part="snippet,contentDetails,statistics",
                            id=video_id)
    response=request.execute()

    for item in response["items"]:
            data = dict(
               Channel_Name=item['snippet']['channelTitle'],
               Channel_Id=item['snippet']['channelId'],
               Video_ID=item['id'],
               Title=item['snippet']['title'],
               Tags=item['snippet'].get('tags',['na']),
               Thumbnail=item['snippet']['thumbnails']['default']['url'],
               Description=item['snippet'].get('description',['na']),
               Published_Date=item['snippet']['publishedAt'],
               Duration=item['contentDetails']['duration'],
               Views=item['statistics'].get('viewCount',0),
               Likes = item['statistics'].get('likeCount',0),
               Comments=item['statistics'].get('commentCount',0),
               Favorite_Count=item['statistics']['favoriteCount'],
               Definition=item['contentDetails']['definition'],
               Caption_status=item['contentDetails']['caption']
            )
                
            video_data.append(data)
  return video_data

#GET PLAYLIST

def get_playlist_details(channel_id):

  next_page_token=None
  All_data=[]
  while True:
    request=yt.playlists().list(part='snippet,contentDetails',
                                channelId=channel_id,
                                maxResults=50,
                                pageToken=next_page_token)
    response=request.execute()
    for item in response["items"]:
      data=dict(Playlist=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                Video_count=item['contentDetails']['itemCount'])
      All_data.append(data)

    next_page_token=response.get('nextPageToken')
    if next_page_token is None:
      break
  return All_data

#GET COMMENT DETAILS

def get_coment_info(video_ids):
    comment_data = []

    for video_id in video_ids:
        try:
            request = yt.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response["items"]:
                comment_data.append({
                    'Comment_Id': item['snippet']['topLevelComment']['id'],
                    'Video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                })

        except Exception as e:
            print(f"Error fetching comments for video {video_id}: {e}")

    return comment_data


#INSERT TO MONGODB



import pymongo

# Connect to MongoDB running on localhost
client = pymongo.MongoClient("mongodb://localhost:27017/")

db=client['Youtube_data']

#CALL ALL FUNCTION


def channel_details(channel_id):
  ch_details=get_channel_info(channel_id)
  pl_details=get_playlist_details(channel_id)
  vi_ids=get_video_ids(channel_id)
  vi_details=get_video_details(vi_ids)
  com_details=get_coment_info(vi_ids)

  coll1=db['channel_details']
  coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                    "comment_information":com_details})
  return 'upload done'

def insert_channel_details(channel_info):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Youtube_data']
    coll = db['channel_details']
    coll.insert_one(channel_info)

#CREATE COMMENT TABLE

#table creation for channels
def channels_table():
    host = "localhost"
    user = "root"
    password = "9xKqdXaOQnkn-y"
    database = "youtube_data"

    mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    cursor=mydb.cursor() 

    drop_query='''drop table if exists channel'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channel(Channel_name varchar(100),
                                                            Channel_id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_videos int,
                                                            Description text,
                                                            Playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print('channel already created')    

    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)


    for a,row in df.iterrows():
        insert_query='''insert into channel(Channel_name,
                                            Channel_id ,
                                            Subscribers ,
                                            Views ,
                                            Total_videos ,
                                            Description ,
                                            Playlist_id) 
                                            values(%s,%s,%s,%s,%s,%s,%s)  '''
        values = (
            row['Channel_name'],
            row['Channel_id'],
            row['Subscribers'],
            row['Views'],
            row['Total_videos'],
            row['Description'],
            row['Playlist_id']
        )
    
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('already table inserted')    


#PLAYLIST TABLE
def convert_date(date_str):
    # Parse the date string to a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
    # Format the datetime object as 'YYYY-MM-DD'
    formatted_date = date_obj.strftime('%Y-%m-%d')
    return formatted_date
def playlist_table():
    host = "localhost"
    user = "root"
    password = "9xKqdXaOQnkn-y"
    database = "youtube_data"

    mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
    cursor=mydb.cursor() 
    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()




    create_query='''create table if not exists playlists(Playlist varchar(100) primary key,
                                                        Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt date,
                                                        Video_count int)'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']

    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)  


    for a,row in df1.iterrows():
            insert_query='''insert into playlists(Playlist,
                                                Title ,
                                                Channel_Id ,
                                                Channel_Name ,
                                                PublishedAt ,
                                                Video_count 
                                                ) 
                                                values(%s,%s,%s,%s,%s,%s)  '''
            values = (
                row['Playlist'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                convert_date(row['PublishedAt']),
                row['Video_count']
            )
        
    
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print('already table inserted')    
            

#CREATE VIDEO TABLE

def convert_date(date_str):
    # Parse the date string to a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
    # Format the datetime object as 'YYYY-MM-DD'
    formatted_date = date_obj.strftime('%Y-%m-%d')
    return formatted_date

def convert_duration(duration_str):
    duration = duration_str[2:]  # Remove 'PT' at the beginning
    hours = 0
    minutes = 0
    seconds = 0
    if 'H' in duration:
        hours, duration = duration.split('H')
    if 'M' in duration:
        minutes, duration = duration.split('M')
    if 'S' in duration:
        seconds = duration.replace('S', '')
    return f'{hours}:{minutes}:{seconds}'

def videos_table():
    # MongoDB connection
    client = MongoClient("mongodb://localhost:27017/")
    db = client['Youtube_data']
    collection = db['channel_details']

    # MySQL connection
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="9xKqdXaOQnkn-y",
        database="youtube_data",
        charset='utf8mb4'
    )
    cursor = mydb.cursor()

    # Drop the table if it exists
    drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)
    mydb.commit()

    # Create the table
    create_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            Channel_Name VARCHAR(150),
            Channel_Id VARCHAR(100),
            Video_ID VARCHAR(50) PRIMARY KEY,
            Title VARCHAR(150),
            Tags TEXT,
            Thumbnail VARCHAR(225),
            Description TEXT,
            Published_Date DATE,
            Duration TIME,
            Views INT,
            Likes INT,
            Comments INT,
            Favorite_Count INT,
            Definition VARCHAR(10),
            Caption_status varchar(50)
        )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    '''
    cursor.execute(create_query)
    mydb.commit()

    # Retrieve data from MongoDB and insert into MySQL
    vi_list = []
    for vi_data in collection.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''
            INSERT INTO videos (
                Channel_Name, Channel_Id, Video_ID, Title, Tags, Thumbnail, Description,
                Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['Channel_Name'], row['Channel_Id'], row['Video_ID'], row['Title'], json.dumps(row['Tags']),
            row['Thumbnail'], row['Description'],convert_date(row['Published_Date']), convert_duration(row['Duration']), row['Views'],
            row['Likes'], row['Comments'], row['Favorite_Count'], row['Definition'], row['Caption_status']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")

  
    

#CREATE COMMENT TABLE
def convert_date(date_str):
    # Parse the date string to a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
    # Format the datetime object as 'YYYY-MM-DD'
    formatted_date = date_obj.strftime('%Y-%m-%d')
    return formatted_date

def comment_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="9xKqdXaOQnkn-y",
        database="youtube_data")
    cursor = mydb.cursor()

    # Drop existing table
    drop_query = '''DROP TABLE IF EXISTS COMMENTS'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create new table
    create_query = '''CREATE TABLE IF NOT EXISTS COMMENTS (
        Comment_Id VARCHAR(100) PRIMARY KEY,
        Video_Id VARCHAR(50),
        Comment_Text TEXT,
        Comment_Author VARCHAR(150),
        Comment_Published DATE
    )'''
    cursor.execute(create_query)
    mydb.commit()

    
    # Retrieve data from MongoDB
    com_list = []
    db=client['Youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    # Create DataFrame from MongoDB data
    df3 = pd.DataFrame(com_list)

    # Insert values into MySQL table
    for index, row in df3.iterrows():
        insert_query = '''
            INSERT INTO comments (
                Comment_Id, 
                Video_Id,
                Comment_Text,
                Comment_Author,
                Comment_Published
            )
            VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            row['Comment_Id'],
            row['Video_Id'],
            json.dumps(row['Comment_Text']),
            row['Comment_Author'],
            convert_date(row['Comment_Published'])
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting comment: {err}")
    


def all_tables():
    channels_table()
    playlist_table()
    videos_table()
    comment_table()

    return "Tables created successfully"


def show_channels_table():
  cl_list=[]
  db = client["Youtube_data"]
  coll1 = db["channel_details"]
  for cl_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
      cl_list.append(cl_data["channel_information"])
  df = st.dataframe(cl_list)

  return df

def show_playlists_table():
  pl_list = []
  db = client["Youtube_data"]
  coll1 = db["channel_details"]
  for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
      for i in range(len(pl_data["playlist_information"])):
        pl_list.append(pl_data["playlist_information"][i])
  df1 = st.dataframe(pl_list)

  return df1

def show_videos_table():
  vi_list = []
  db = client["Youtube_data"]
  coll1 = db["channel_details"]
  for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
      for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])
  df2 = st.dataframe(vi_list)

  return df2

def show_comments_table():
  com_list = []
  db = client["Youtube_data"]
  coll1 = db["channel_details"]
  for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
      for i in range(len(com_data["comment_information"])):
         com_list.append(com_data["comment_information"][i])
  df3 = st.dataframe(com_list)

  return df3



#streamlit code
with st.sidebar:
  st.sidebar.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
  st.sidebar.header("Project benefits")
  st.sidebar.caption("Explore the YouTube API: Learn how to access and fetch data from YouTube's extensive API...")
channel_id = st.sidebar.text_input("ENTER NEW CHANNEL ID") 

if st.button("COLLECT AND STORE DATA IN MONGODB"):
  ch_ids=[]
  db = client["Youtube_data"]
  coll1 = db["channel_details"]
  for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
    ch_ids.append(ch_data["channel_information"]["Channel_id"])

  if channel_id in ch_ids:
          st.sidebar.warning("CHANNEL DETAILS OF THE GIVEN CHANNEL ID ALREADY EXISTS ")
  else:
        insert = channel_details(channel_id)
        st.sidebar.success(insert)


if st.button("MIGRATE TO MYSQL"):
    Table = all_tables()
    st.sidebar.success(Table)

show_table = st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table =="CHANNELS":
   show_channels_table()

elif show_table =="PLAYLISTS":
   show_playlists_table()

elif show_table == "VIDEOS":
   show_videos_table()

elif show_table =="COMMENTS":
   show_comments_table()



#sql connection

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="9xKqdXaOQnkn-y",
        database="youtube_data")
cursor = mydb.cursor()

Question = st.selectbox("EXECUTE QUESTION",    ("1.What are the names of all the videos and their corresponding channels?",
                                                "2.Which channels have the most number of videos, and how many videos do they have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?" ))


if Question == '1.What are the names of all the videos and their corresponding channels?':
   query1 ='''select channel_name as channelname ,title as Videotitle from videos '''
   cursor.execute(query1)
   
   t1=cursor.fetchall()
   mydb.commit()
   st.write(pd.DataFrame(t1,columns=["CHANNEL NAME","VIDEO TITLE"]))
   

elif Question == '2.Which channels have the most number of videos, and how many videos do they have?':
   query2 ='''select channel_name as channelname,total_videos as no_of_videos from channel 
           order by total_videos desc'''
   cursor.execute(query2)
   
   t2=cursor.fetchall()
   mydb.commit()
   st.write(pd.DataFrame(t2,columns=["CHANNEL NAME","NO OF VIDEOS"]))
   

elif Question ==  '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    
    t3 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t3, columns = ["VIEWS","CHANNEL NAME","VIDEO TITLE"]))

elif Question == '4.How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as Nocomments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    
    t4=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t4, columns=["NO COMMENTS", "VIDEO TITLE"]))
   

elif Question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    
    t5 = cursor.fetchall()
    mydb.commit()
    t5_df = pd.DataFrame(t5, columns=["VIDEO TITLE", "CHANNEL NAME", "LIKE COUNT"])
    t5_df['LIKE COUNT'] = t5_df['LIKE COUNT'].astype(bytes)
    # Display the DataFrame in Streamlit
    st.write(t5_df)
   

elif Question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    
    t6 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t6, columns=["LIKE COUNT","VIDEO TITLE"]))
    

elif Question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channel;"
    cursor.execute(query7)
   
    t7=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t7, columns=["CHANNEL NAME","TOTAL VIEWS"]))
   

elif Question == '8.What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
   
    t8=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t8,columns=["NAME", "VIDEO PUBLISHED ON", "CHANNELNAME"]))


elif Question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    
    t9=cursor.fetchall()
    mydb.commit()
    t9 = pd.DataFrame(t9, columns=['CHANNELTITLE', 'AVERAGEDURATION'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['CHANNELTITLE']
        average_duration = row['AVERAGEDURATION']
        average_duration_str = str(average_duration)
        T9.append({"'CHANNELTITLE'": channel_title ,  "AVERAGEDURATION": average_duration_str})
    st.write(pd.DataFrame(T9))
    

elif Question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
   query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
   cursor.execute(query10)
   
   t10=cursor.fetchall()
   mydb.commit()
   st.write(pd.DataFrame(t10, columns=['VIDEO TITLE', 'CHANNEL NAME', 'NO OF COMMENTS']))




                                