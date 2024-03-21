#import the needed modules
from googleapiclient.discovery import build
import pymongo
import pandas as pd
import re
from datetime import datetime
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu 
from PIL import Image

#API key 
google_api = "AIzaSyC_bzjkSBcAKjpgaq82SHBwZsizeMHnolk"

# mongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['YOUTUBE']

# Read the content of the text file
with open(r"C:\Users\gowth\Downloads\Projects_gowtham\read.txt", "r") as file:
    text_content = file.read()

#Image and the title.
img = Image.open(r'C:\Users\gowth\Downloads\Projects_gowtham\images\youtube-logo-9.png')
st.image(img, channels = "RBG")
st.title("Youtube Data Harvesting and Warehousing")

#Calling the API 
def Api_connect():
    Api_Id = google_api

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey = Api_Id)
    return youtube

youtube = Api_connect()   


#get channels information
def get_channel_info(channel_id):

    channel_response = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id).execute()
    
    channel_table = {
            'Channel_Name' : channel_response['items'][0]["snippet"]["title"],
            'Channel_Id' : channel_response['items'][0]['id'],
            'Subscribers' : channel_response['items'][0]['statistics']['subscriberCount'],
            'Views' : channel_response['items'][0]['statistics']['videoCount'],
            'Total_Videos':channel_response['items'][0] ['statistics']['viewCount'],
            'Channel_description': channel_response['items'][0]['snippet']['description'],
            'Playlist_Id' :channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']}

    return channel_table 


#playlist information
def channel_playlist(channel_id):
    next_page_token = None
    
    playlist = []

    while True:
        request = youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
        )
        channel_data = request.execute()

        for item in channel_data['items']:
            data = dict(
                        Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet'].get('publishedAt'),
                        Video_Count= item['contentDetails']['itemCount'])
            
            playlist.append(data)
        
        next_page_token = channel_data.get('nextPageToken')
        if next_page_token is None:
            break

    return playlist


#Getting the ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#Converting the time_format
def duration_to_sec(due):
    match = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", due)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    if hours == 0:
        return (f'{minutes}mins : {seconds}sec')
    else:
        return (f'{hours}hrs : {minutes}mins:{seconds}sec')
    
#Vidoe information 
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            duration = item['contentDetails']['duration']
            duration_formatted = duration_to_sec(duration)
            
            data = {
                'Channel_Name': item['snippet']['channelTitle'],
                'Channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Title': item['snippet']['title'],
                'Tags': item['snippet'].get('tags')[0] if item['snippet'].get('tags') else None,
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet']['description'],
                'Published_Date': item['snippet']['publishedAt'],
                'Duration': item['contentDetails']['duration'],
                'Duration_timeformat': duration_formatted,
                'Views': item['statistics'].get('viewCount'),
                'Likes': item['statistics'].get('likeCount'),
                'Comments': item['statistics'].get('commentCount'),
                'Favorite_Count': item['statistics']['favoriteCount'],
                'Definition': item['contentDetails']['definition'],
                'Caption_Status': item['contentDetails']['caption']
            }
            video_data.append(data)
    return video_data


#Comment details
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response["items"]:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet'].get('publishedAt'))
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data


#assigning to the mongo DB
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    ch_playlist=channel_playlist(channel_id)
    vd_id = get_videos_ids(channel_id)
    vd_details = get_video_info(vd_id)
    cmt_details = get_comment_info(vd_id)

    collection_details = db['ch_details']
    collection_details.insert_one({
        "channel_information":ch_details,
        "playlist_information":ch_playlist, 
        'video_information':vd_details,
        "comment information" : cmt_details})

    return "upload completed successful"

#creating and inserting channel tables in MYSQL database 
def channel_table(one_a):

    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user='root',
        password='root',
        database = "youtube"
        )
    mycursor=mydb.cursor(buffered=True)

    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
    mycursor.execute(create_query)
    mydb.commit()

     #fetching all datas
    query_1= "SELECT * FROM channels"
    mycursor.execute(query_1)
    table= mycursor.fetchall()
    mydb.commit()

    chann_list= []
    chann_list2= []
    df_all_channels= pd.DataFrame(table)

    chann_list.append(df_all_channels[0])
    for i in chann_list[0]:
        chann_list2.append(i)
    

    if one_a in chann_list2:
        news= f"Your Provided Channel {one_a} is Already exists"        
        return news
    
    else:

        single_channel_details= []
        db = client["YOUTUBE"]
        coll1=db["ch_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name":one_a},{"_id":0}):
            single_channel_details.append(ch_data["channel_information"])

        df= pd.DataFrame(single_channel_details)

        #assigning to the sql database

        for index,row in df.iterrows():
            insert_channel_query = '''INSERT INTO channels(channel_name,
                                                            channel_id,
                                                            subscribers,
                                                            Channel_Description,
                                                            total_videos,
                                                            playlist_id,
                                                            views)
                                                            
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s)'''
            
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row["Channel_description"],
                    row['Total_Videos'],
                    row['Playlist_Id'],
                    row['Views'])
                            
            
            mycursor.execute(insert_channel_query,values)
            mydb.commit()

#creating and inserting playlist tables in MYSQL database
def playlist_table(one_a):

    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user='root',
        password='root',
        database = "youtube"
        )
    mycursor=mydb.cursor(buffered=True)

    create_query='''create table if not exists playlist(Playlist_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_Count int)'''
    mycursor.execute(create_query)
    mydb.commit()

    single_ch_playlist=[]
    db = client['YOUTUBE']
    col=db['ch_details']
    for pl_data in col.find({"channel_information.Channel_Name":one_a},{'_id':0,}):
          single_ch_playlist.append(pl_data["playlist_information"])
    df_0 =pd.DataFrame(single_ch_playlist[0])

    # Assuming 'PublishedAt' is a column in df_1
    df_0['PublishedAt'] = pd.to_datetime(df_0['PublishedAt'], format='%Y-%m-%dT%H:%M:%SZ')

    # Now, 'PublishedAt' in df_1 is a datetime object
    # You can use strftime to convert it to the desired format for MySQL
    df_0['PublishedAt'] = df_0['PublishedAt'].dt.strftime('%Y-%m-%d %H:%M:%S')

    
    for index, row in df_0.iterrows(): 
        
        insert_query='''insert into playlist(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
            
            
        values=(row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()

#creating and inserting video tables in MYSQL database    
def videos_table(one_a):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user='root',
        password='root',
        database = "youtube"
        )
    mycursor=mydb.cursor(buffered=True)

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration varchar(100),
                                                    Duration_timeformat varchar(100),
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()

    single_ch_video = []
    db = client["YOUTUBE"]
    coll1 = db['ch_details']
    for vi_data in coll1.find({"channel_information.Channel_Name":one_a},{"_id":0}):
            single_ch_video.append(vi_data['video_information'])

    
    df_1 = pd.DataFrame(single_ch_video[0]) 

    #to remove Tag consist of list
    df_1['Tags'] = df_1['Tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    # Assuming 'Published_Date' is a column in df_1
    df_1['Published_Date'] = pd.to_datetime(df_1['Published_Date'], format='%Y-%m-%dT%H:%M:%SZ')

    # Now, 'Published_Date' in df_1 is a datetime object
    # You can use strftime to convert it to the desired format for MySQL
    df_1['Published_Date'] = df_1['Published_Date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    for index,row in df_1.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Duration_timeformat,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            
            values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'], 
                        row['Published_Date'],
                        row['Duration'],
                        row['Duration_timeformat'], 
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                        )
                        
                
            mycursor.execute(insert_query,values)
            mydb.commit()

#creating and inserting comment tables in MYSQL database
def comments_table(one_a):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user='root',
        password='root',
        database = "youtube"
        )
    mycursor=mydb.cursor(buffered=True)

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published timestamp
                                                            )'''

                                                    
    mycursor.execute(create_query)
    mydb.commit()

    single_ch_comments = []
    db = client["YOUTUBE"]
    coll1 = db['ch_details']
    for cmt_data in coll1.find({"channel_information.Channel_Name":one_a},{"_id":0}):
        single_ch_comments.append(cmt_data['comment information'])

    df_2= pd.DataFrame(single_ch_comments[0]) 

    # Assuming 'Published_Date' is a column in df_2
    df_2['Comment_Published'] = pd.to_datetime(df_2['Comment_Published'], format='%Y-%m-%dT%H:%M:%SZ')

    # Now, 'Comment_Published' in df_2 is a datetime object
    # You can use strftime to convert it to the desired format for MySQL
    df_2['Comment_Published'] = df_2['Comment_Published'].dt.strftime('%Y-%m-%d %H:%M:%S')
 


 
    for index,row in df_2.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''
            
            
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )

            
            mycursor.execute(insert_query,values)
            mydb.commit()          
    
def tables(single_ch):
    news = channel_table(single_ch)
    if news:
        return news
    else:
        playlist_table(single_ch)
        videos_table(single_ch)
        comments_table(single_ch)

        return "table created successful"

#function to call the channel detail in streamlit
def show_channel():
    ch_list = []

    db = client["YOUTUBE"]
    coll1 = db['ch_details']
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        print(ch_data)
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list) 

    return df

#function to call the playlist detail in streamlit
def show_playlist():
    playlist_details=[]
    db = client['YOUTUBE']
    col=db['ch_details']
    for pl_data in col.find({},{'_id':0, 'playlist_information':1}):
          playlist_details.append(pl_data["playlist_information"])
    df_0 =pd.DataFrame(playlist_details[0])
        # Assuming 'PublishedAt' is a column in df_1
    df_0['PublishedAt'] = pd.to_datetime(df_0['PublishedAt'], format='%Y-%m-%dT%H:%M:%SZ')

    # Now, 'PublishedAt' in df_1 is a datetime object
    # You can use strftime to convert it to the desired format for MySQL
    df_0['PublishedAt'] = df_0['PublishedAt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_0 =st.write(df_0)
    return df_0

#function to call the vidoes detail in streamlit
def show_videos_table(): 
    video_list = []
    db = client["YOUTUBE"]
    coll1 = db['ch_details']
    for vi_data in coll1.find({},{"_id":0,'video_information':1}):
        for i in range(len(vi_data["video_information"])):
            video_list.append(vi_data['video_information'][i])
       
    df_1 = pd.DataFrame(video_list) 
    #to remove Tag consist of list
    df_1['Tags'] = df_1['Tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    # Assuming 'Published_Date' is a column in df_1
    df_1['Published_Date'] = pd.to_datetime(df_1['Published_Date'], format='%Y-%m-%dT%H:%M:%SZ')

    # Now, 'Published_Date' in df_1 is a datetime object
    # You can use strftime to convert it to the desired format for MySQL
    df_1['Published_Date'] = df_1['Published_Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_1 = st.write(df_1) 
    
    return df_1

#function to call the comments detail in streamlit
def show_comments():
    comments_list = []
    db = client["YOUTUBE"]
    coll1 = db['ch_details']
    for cmt_data in coll1.find({},{"_id":0,'comment information':1}):
        comments_list.append(cmt_data['comment information'])
    df_2= pd.DataFrame(comments_list[0]) 

    # Assuming 'Published_Date' is a column in df_2
    df_2['Comment_Published'] = pd.to_datetime(df_2['Comment_Published'], format='%Y-%m-%dT%H:%M:%SZ')

    # Now, 'Comment_Published' in df_2 is a datetime object
    # You can use strftime to convert it to the desired format for MySQL
    df_2['Comment_Published'] = df_2['Comment_Published'].dt.strftime('%Y-%m-%d %H:%M:%S')    

    df_2= st.write(df_2) 
    return df_2

#menu bar                 
selected = option_menu(
    menu_title = None,
    options = ["ReadMe", "Project", "Query"],
    icons = ["book", "calendar3-event", "chat-left"],
    default_index = 0,
    orientation=  "horizontal")

#Options in the menu bar
if selected == "ReadMe":
    st.header("ABOUT THE PROJECT")
    st.write(text_content, height=500)

if selected == "Project":
    channel_id = st.text_input("Enter the channel ID")
    if st.button("collect and store data"):
        ch_ids=[]
        db=client["YOUTUBE"]
        coll1=db["ch_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
            
        if channel_id in ch_ids:
            st.error("Channel Details of the given channel id already exists", icon = '🙅‍♂️')

        else:
            insert= channel_details(channel_id)
            st.success(insert, icon="✅")

    all_channels= []
    db = client["YOUTUBE"]
    coll1=db["ch_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        all_channels.append(ch_data["channel_information"]["Channel_Name"])
            
    unique_channel = st.selectbox("Select the Channel",all_channels)

    st.header("Migration to SQL")
    if st.button("Migrate to Sql"):
        Table = tables(unique_channel)
        if Table == unique_channel:
            st.success(Table, icon="✅")
        else:st.error(Table, icon='🙅‍♂️')    

    show_table=st.radio("SELECT THE TABLE",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

    if show_table=="CHANNELS":
        show_channel()
        st.success("DONE", icon="✅")

    elif show_table=="PLAYLISTS":
        show_playlist()
        st.success("DONE", icon="✅")

    elif show_table=="VIDEOS":
        show_videos_table()
        st.success("DONE", icon="✅")

    elif show_table=="COMMENTS":
        show_comments()
        st.success("DONE", icon="✅")
            
if selected == "Query":
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user='root',
        password='root',
        database = "youtube"
        )
    mycursor = mydb.cursor()

    question = st.selectbox("Select your question",("1. All the videos and the channel name",
                                                        "2. channels with most number of videos",
                                                        "3. 10 most viewed videos",
                                                        "4. comments in each videos",
                                                        "5. Videos with higest likes",
                                                        "6. likes of all videos",
                                                        "7. views of each channel",
                                                        "8. videos published in the year of 2022",
                                                        "9. average duration of all videos in each channel",
                                                        "10. videos with highest number of comments"))
   
    #Note : I throwdown a error while I using the mydb.commit 
    if question=="1. All the videos and the channel name":
        query1='''select title as videos,channel_name as channelname from videos'''
        mycursor.execute(query1)
        #mydb.commit()
        tables_1=mycursor.fetchall()
        df=pd.DataFrame(tables_1,columns=["video title","channel name"])
        st.write(df)
        st.success("DONE", icon="✅")

    elif question=="2. channels with most number of videos":
        query2='''select channel_name as channelname,total_videos as no_videos from channels 
                    order by total_videos desc'''
        mycursor.execute(query2)
        #mydb.commit()
        t2=mycursor.fetchall()
        df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
        st.write(df2)
        st.success("DONE", icon="✅")

    elif question=="3. 10 most viewed videos":
        query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                    where views is not null order by views desc limit 10'''
        mycursor.execute(query3)
        #mydb.commit()
        t3=mycursor.fetchall()
        df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
        st.write(df3)
        st.success("DONE", icon="✅")

    elif question=="4. comments in each videos":
        query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
        mycursor.execute(query4)
        #mydb.commit()
        t4=mycursor.fetchall()
        df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
        st.write(df4)
        st.success("DONE", icon="✅")

    elif question=="5. Videos with higest likes":
        query5='''select title as videotitle,channel_name as channelname,likes as likecount
                    from videos where likes is not null order by likes desc'''
        mycursor.execute(query5)
        #mydb.commit()
        t5=mycursor.fetchall()
        df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
        st.write(df5)
        st.success("DONE", icon="✅")

    elif question=="6. likes of all videos":
        query6='''select likes as likecount,title as videotitle from videos'''
        mycursor.execute(query6)
        #mydb.commit()
        t6=mycursor.fetchall()
        df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
        st.write(df6)
        st.success("DONE", icon="✅")

    elif question=="7. views of each channel":
        query7='''select channel_name as channelname ,views as totalviews from channels'''
        mycursor.execute(query7)
        #mydb.commit()
        t7=mycursor.fetchall()
        df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
        st.write(df7)
        st.success("DONE", icon="✅")

    elif question=="8. videos published in the year of 2022":
        query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                    where extract(year from published_date)=2022'''
        mycursor.execute(query8)
        #mydb.commit()
        t8=mycursor.fetchall()
        df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
        st.write(df8)
        st.success("DONE", icon="✅")

    elif question=="9. average duration of all videos in each channel":
        query9='''select channel_name as channelname,AVG(Duration_timeformat) as averageduration from videos group by channel_name'''
        mycursor.execute(query9)
        #mydb.commit()
        t9=mycursor.fetchall()
        df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

        T9=[]
        for index,row in df9.iterrows():
            channel_title=row["channelname"]
            average_duration=row["averageduration"]
            average_duration_str=str(average_duration)
            T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df1=pd.DataFrame(T9)
        st.write(df1)
        st.success("DONE", icon="✅")

    elif question=="10. videos with highest number of comments":
        query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                    not null order by comments desc'''
        mycursor.execute(query10)
        #mydb.commit()
        t10=mycursor.fetchall()
        df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
        st.write(df10)
        st.success("DONE", icon="✅")        

