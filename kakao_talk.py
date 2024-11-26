from flask import Flask, request, jsonify
import pymysql
import pandas as pd

# initialize app
app = Flask(__name__)


# get someone's friend_list; friend's name, image url
@app.route('/friend_list', methods=['POST'])
def friend_list_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='kakaotalk')
    sql = (
            """
            SELECT c.name, ifnull(p.url, "images/0.png") url
            FROM T_friend f
            JOIN T_customer c on f.friend_id = c.cust_id
            LEFT JOIN T_picture_update pu on c.cust_id = pu.cust_id
            LEFT JOIN T_picture p on pu.max_pic_id = p.pic_id
            WHERE f.cust_id = %s
            ORDER BY c.name;
            """
            % cust_input
    )

    df = pd.read_sql_query(sql, conn)
    df_dict = {"name": df['name'].tolist(), "image": df['url'].tolist()}

    return jsonify(df_dict)


# get updated friend list; recent 2 month
@app.route('/update_friends', methods=['POST'])
def update_friends_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='kakaotalk')
    sql = (
            """
            select tc.name, tp.url from t_friend tf
            join t_customer tc on tf.friend_id = tc.cust_id
            join t_picture_update tpu on tpu.cust_id = tc.cust_id
            join t_picture tp on tp.pic_id = tpu.max_pic_id
            where tf.cust_id = %s and timestampdiff(month, tp.update_time, '2023-10-28') < 2
            order by tpu.max_pic_id desc;
            """
            % cust_input
    )

    df = pd.read_sql_query(sql, conn)
    df_dict = {"name": df['name'].tolist(), "image": df['url'].tolist()}

    return jsonify(df_dict)


@app.route('/birthday', methods=['POST'])
def birthday_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='kakaotalk')
    # to see the output, 2024-10-03, original curdate()
    sql_today = (
            """
            select tc.name, ifnull(tp.url, 'images/0.png') url from t_friend tf
            join t_customer tc on tf.friend_id = tc.cust_id
            left join t_picture_update tpu on tpu.cust_id = tf.friend_id
            left join t_picture tp on tp.pic_id = tpu.max_pic_id
            where tf.cust_id = %s and date_format(birthday, '%%m %%d') = date_format('2024-10-03', '%%m %%d')
            order by tc.name;
            """
            % cust_input
    )
    df_today = pd.read_sql_query(sql_today, conn)

    sql_past = (
            """
            select tc.name, ifnull(tp.url, 'images/0.png') url from t_friend tf
            join t_customer tc on tf.friend_id = tc.cust_id
            left join t_picture_update tpu on tpu.cust_id = tf.friend_id
            left join t_picture tp on tp.pic_id = tpu.max_pic_id
            where tf.cust_id = %s and concat(year(curdate()), '-', date_format(birthday, '%%m-%%d')) between date_sub(curdate(), interval 30 day) AND date_sub(curdate(), interval 1 day)
            order by concat(year(curdate()), '-', date_format(tc.birthday, '%%m-%%d')), tc.name;
            """
            % cust_input
    )
    df_past = pd.read_sql_query(sql_past, conn)

    sql_coming = (
            """
            select tc.name, ifnull(tp.url, 'images/0.png') url from t_friend tf
            join t_customer tc on tf.friend_id = tc.cust_id
            left join t_picture_update tpu on tpu.cust_id = tf.friend_id
            left join t_picture tp on tp.pic_id = tpu.max_pic_id
            where tf.cust_id = %s and concat(year(curdate()), '-', date_format(tc.birthday, '%%m-%%d')) between date_add(curdate(), interval 1 day) AND date_add(curdate(), interval 30 day)
            order by concat(year(curdate()), '-', date_format(tc.birthday, '%%m-%%d')), tc.name;
            """
            % cust_input
    )

    df_coming = pd.read_sql_query(sql_coming, conn)

    df_dict = {
        "today": {"name": df_today['name'].tolist(), "image": df_today['url'].tolist()},
        "past": {"name": df_past['name'].tolist(), "image": df_past['url'].tolist()},
        "coming": {"name": df_coming['name'].tolist(), "image": df_coming['url'].tolist()}
    }

    return jsonify(df_dict)


@app.route('/recommend', methods=['POST'])
def recommend_friend_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    with pymysql.connect(host='localhost', port=3306, user='root', password='root', db='kakaotalk') as conn:
        sql_to_me = (
            """
            select tc.name, ifnull(tp.url, 'images/0.png') url from t_friend tf
            join t_customer tc on tf.cust_id = tc.cust_id
            left join t_picture_update tpu on tpu.cust_id = tc.cust_id
            left join t_picture tp on tp.pic_id = tpu.max_pic_id
            where tf.friend_id = %s and tf.cust_id not in 
                (select friend_id from t_friend
                where cust_id = %s)
            order by tc.name;
            """
        )
        df_to_me = pd.read_sql_query(sql_to_me, conn, params=[cust_input, cust_input])

        sql_popular = (
            """
            select tc.name, ifnull(tp.url, 'images/0.png') url from (
                select friend_id, count(friend_id) cnt from t_friend
                where cust_id in 
                    (select friend_id from t_friend
                    where cust_id = %s)
                group by friend_id
            ) a
            join t_customer tc on tc.cust_id = a.friend_id
            left join t_picture_update tpu on tpu.cust_id = a.friend_id
            left join t_picture tp on tp.pic_id = tpu.max_pic_id
            where a.friend_id not in
                (select friend_id from t_friend
                where cust_id = %s)
                and cnt >= 10
            order by a.cnt desc;
            """
        )
        df_popular = pd.read_sql_query(sql_popular, conn, params=[cust_input, cust_input])

    df_dict = {
        "to_me": {"name": df_to_me['name'].tolist(), "image": df_to_me['url'].tolist()},
        "popular": {"name": df_popular['name'].tolist(), "image": df_popular['url'].tolist()}
    }

    return jsonify(df_dict)


@app.route('/chat_detail', methods=['POST'])
def chat_detail_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    date_input = request_json['date']
    room_input = request_json['room_id']
    with pymysql.connect(host='localhost', port=3306, user='root', password='root', db='kakaotalk') as conn:
        sql_cnt = (
            """
            select count(cust_id) as cnt from t_chat_member
            where room_id = %s
            group by room_id;
            """
        )
        # to prevent sql injection, the following methods are recommended
        df_cnt = pd.read_sql_query(sql_cnt, conn, params=[room_input])

        sql_chat = (
            """
            select tcu.name, ifnull(tp.url, 'images/0.png') url, tc.chat, date_format(tc.chat_time, '%%p %%h:%%i') chat_time, if(tcu.cust_id = %s, 1, 0) me from t_chat tc
            join t_customer tcu on tc.cust_id = tcu.cust_id
            left join t_picture_update tpu on tpu.cust_id = tcu.cust_id
            left join t_picture tp on tp.pic_id = tpu.max_pic_id
            where tc.room_id = %s and date(tc.chat_time) = %s
            order by tc.chat_id;
            """
        )
        df_chat = pd.read_sql_query(sql_chat, conn, params=[cust_input, room_input, date_input])

    df_dict = {
        "room_id": room_input, "date": date_input, "cust_id": cust_input,
        "count": df_cnt['cnt'].tolist()[0] if not df_cnt.empty else 0,
        "chats": {
            "chat": df_chat['chat'].tolist(),
            "name": df_chat['name'].tolist(),
            "image": df_chat['url'].tolist(),
            "me": df_chat['me'].tolist()
        }
    }
    return jsonify(df_dict)

# A query assuming that t_chat_member table is a table that manages the entire chat room.
# in the lecture, there was an error in printing a room where cust_id did not belong (in the above assumption)
# however, if t_chat_member is the chat room table to which the cust_id belongs, the query of the lecture is correct
@app.route('/chat_list', methods=['POST'])
def chat_list_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    with pymysql.connect(host='localhost', port=3306, user='root', password='root', db='kakaotalk') as conn:
        sql = (
            """
            select t1.room_id, if(count(*)>3, concat(substring_index(group_concat(tc.name), ',', 3), ', ...'), 
            substring_index(group_concat(tc.name), ',', 3)) as names, 
            t1.chat, date_format(t1.chat_time, '%%m-%%d %%p %%h:%%i') as chat_time from t_chat t1
            join t_chat_member tcm on tcm.room_id = t1.room_id and tcm.cust_id != %s
            join t_customer tc on tc.cust_id = tcm.cust_id
            where (t1.room_id, t1.chat_id) in(
                select t2.room_id, max(t2.chat_id) recent from t_chat t2
                where room_id in(
                    select room_id from t_chat_member
                    where cust_id = %s
                )
                group by t2.room_id
            )
            group by t1.chat_id
            order by t1.chat_id desc;
            """
        )

        df = pd.read_sql_query(sql, conn, params=[cust_input])

        df_dict = {
            "names" : df['names'].tolist(),
            "chats" : df['chat'].tolist(),
            "chat_time" : df['chat_time'].tolist(),
        }
    return jsonify(df_dict)

if __name__ == "__main__":
    app.run(debug=True)
