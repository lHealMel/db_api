from flask import Flask, request
import pymysql
import pandas as pd

# initialize app
app = Flask(__name__)

# get someone's friend_list; friend's name, image url
@app.route('/friend_list', methods=['POST'])
def friend_list_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    conn = pymysql.connect(host='localhost', port=3306, user='root',password='root', db='kakaotalk')
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

    return df_dict

# get updated friend list; recent 2 month
@app.route('/update_friends', methods=['POST'])
def update_friends_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    conn = pymysql.connect(host='localhost', port=3306, user='root',password='root', db='kakaotalk')
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

    return df_dict

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
        "today" : {"name" : df_today['name'].tolist(), "image" : df_today['url'].tolist()},
        "past" : {"name" : df_past['name'].tolist(), "image" : df_past['url'].tolist()},
        "coming" : {"name" : df_coming['name'].tolist(), "image" : df_coming['url'].tolist()}
    }

    return df_dict

@app.route('/recommend', methods=['POST'])
def recommend_friend_query():
    request_json = request.get_json()
    cust_input = request_json['cust_id']
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='kakaotalk')
    sql_to_me =(
        """
        select tc.name, ifnull(tp.url, 'images/0.png') url from t_friend tf
        join t_customer tc on tf.cust_id = tc.cust_id
        left join t_picture_update tpu on tpu.cust_id = tc.cust_id
        left join t_picture tp on tp.pic_id = tpu.max_pic_id
        where tf.friend_id = %s and tf.cust_id not in 
            (select friend_id from t_friend
            where cust_id = %s)
        order by tc.name;
        """ % (cust_input, cust_input)
    )
    df_to_me = pd.read_sql_query(sql_to_me, conn)

    sql_popular =(
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
        """ % (cust_input, cust_input)
    )
    df_popular = pd.read_sql_query(sql_popular, conn)

    df_dict = {
        "to_me" : {"name" : df_to_me['name'].tolist(), "image" : df_to_me['url'].tolist()},
        "popular" : { "name" : df_popular['name'].tolist(), "image": df_popular['url'].tolist()}
    }

    return df_dict


if __name__ == "__main__":
    app.run(debug=True)
