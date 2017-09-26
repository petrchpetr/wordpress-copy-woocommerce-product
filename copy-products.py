
import mysql
import mysql.connector
from mysql.connector import Error 

cr_src = {
	'host': '',
	'database': '',
	'user': '',
	'password': ''
}

cr_dst = {
	'host': '',
	'database': '',
	'user': '',
	'password': ''
}

class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None

     
def connect(host,database,user,password):
    """ Connect to MySQL database """
    try:
        conn = mysql.connector.connect(host=host,
                                       database=database,
                                       user=user,
                                       password=password)
        if conn.is_connected():
            print('Connected to MySQL database')
 
    except Error as e:
        print(e)

    return conn 
#    finally:
#        conn.close()
 
def get_products(conn):
    cur = conn.cursor(cursor_class=MySQLCursorDict)
    q = '''
    SELECT * from wp_posts WHERE post_type='product'
    '''
    cur.execute(q)
    for row in cur.fetchall():
        print('get_prod',row['ID'],row['post_title'])
        yield row

def get_variants(conn,vid):
    cur = conn.cursor(cursor_class=MySQLCursorDict)
    q = '''
    SELECT * from wp_posts WHERE post_parent="%d" 
    ''' % vid
    cur.execute(q)
    for row in cur.fetchall():
        print('var',row['ID'],row['post_title'])
        yield row

#def escape_dict(conn,d):
#    return dict([(":%s" % a[0],conn.converter.to_mysql(a[1])) for a in d.items()]) 


def store_post(conn,row):
#    row = escape_dict(conn,post)
    cur = conn.cursor(cursor_class=MySQLCursorDict)
    q = '''
    INSERT INTO wp_posts (
    post_author,
    post_date,
    post_date_gmt,
    post_content,
    post_title,
    post_excerpt,
    post_status,
    comment_status,
    ping_status,
    post_password,
    post_name,
    to_ping,
    pinged,
    post_modified,
    post_modified_gmt,
    post_content_filtered,
    post_parent,
    guid,
    menu_order,
    post_type,
    post_mime_type,
    comment_count
    )
    VALUES(
%(post_author)s,
%(post_date)s,
%(post_date_gmt)s,
%(post_content)s,
%(post_title)s,
%(post_excerpt)s,
%(post_status)s,
%(comment_status)s,
%(ping_status)s,
%(post_password)s,
%(post_name)s,
%(to_ping)s,
%(pinged)s,
%(post_modified)s,
%(post_modified_gmt)s,
%(post_content_filtered)s,
%(post_parent)s,
%(guid)s,
%(menu_order)s,
%(post_type)s,
%(post_mime_type)s,
%(comment_count)s

)
    '''
    cur.execute(q,row)
#    print('store_post ',cur.lastrowid)
    return cur.lastrowid


def get_meta(conn,mid):
    cur = conn.cursor(cursor_class=MySQLCursorDict)
    q = '''
    SELECT * from wp_postmeta WHERE post_id = "%s"
    ''' % mid

    cur.execute(q)
    for row in cur.fetchall():
    #    print('meta ',row)
        yield row

def store_meta(conn,meta):
    cur = conn.cursor(cursor_class=MySQLCursorDict)
#    emeta = escape_dict(conn,meta)
    q = '''
    INSERT INTO wp_postmeta (
    meta_key,meta_value,post_id
    )VALUES(
    %(meta_key)s,
    %(meta_value)s,
    %(post_id)s
    )
    '''
#    print(q)
    cur.execute(q,meta)
    return cur.lastrowid


def get_thumbnail(conn,tid):
    cur = conn.cursor(cursor_class=MySQLCursorDict)
    q = '''
    SELECT * from wp_posts WHERE ID = %s
    ''' % tid
    cur.execute(q)
    return cur.fetchone()
#    for row in cur.fetchall():
#        print(row['ID'])
#        yield row

def store_product(conn,prod):
    return store_post(conn,prod)

def store_thumbnail(conn,thumb):
    return store_post(conn,thumb)


def store_variant(conn,var):
    return store_post(conn,var)

def clean_meta(conn,pid):
    cur = conn.cursor(cursor_class=MySQLCursorDict)
    q = '''
    delete from wp_postmeta WHERE post_id = %s
    ''' % pid
    print(q)
    cur.execute(q)

def clean_post(conn,pid):
    cur = conn.cursor(cursor_class=MySQLCursorDict)
    q = '''
    delete from wp_posts WHERE ID = %s
    ''' % pid
    print(q)
    cur.execute(q)




def clean_up(conn):
    for product in get_products(conn):
        for variant in get_variants(conn,product['ID']):
            clean_meta(conn,variant['ID'])
            clean_post(conn,variant['ID'])
        clean_post(conn,product['ID'])
        clean_meta(conn,product['ID'])
    conn.commit()


def copy_thumb(dst,src,tid,parent):
    dcur = dst.cursor(cursor_class=MySQLCursorDict)
    scur = src.cursor(cursor_class=MySQLCursorDict)
    q = '''
        SELECT * from wp_posts where ID=%s
    ''' % tid
    scur.execute(q)
    th = scur.fetchone()
    th['post_parent'] = parent
    thid = store_variant(dst,th)
    for tmeta in get_meta(src,tid):
        tmeta['post_id'] = thid
        store_meta(dst,tmeta)
    return thid




if __name__ == '__main__':
    src = connect(**cr_src)
    #print(dir(src))
    dst = connect(**cr_dst)
    clean_up(dst)
    for product in get_products(src):
        id = store_product(dst,product)
        variants = {}
        for variant in get_variants(src,product['ID']):
            variant['post_parent'] = id
            vid = store_variant(dst,variant)
            variants[variant['ID']] = vid
#            if variant['post_type'] == 'attachment':
#                thid = vid
#                print('has thumbnail')
            for vmeta in get_meta(src,variant['ID']):
                vmeta['post_id'] = vid
                store_meta(dst,vmeta)
        for meta in get_meta(src,product['ID']):
#            print(meta)
            meta['post_id'] = id
            if(meta['meta_key']=='_thumbnail_id'):
                if meta['meta_value'] in variants:
                    meta['meta_value'] = variants[meta['meta_value']]
                    print('I know thumbnail')
                else:
                    meta['meta_value'] = copy_thumb(dst,src,meta['meta_value'],id)
                    print('I copy thumbnail')
              #  meta['meta_value'] = thid
            store_meta(dst,meta)
#                    tid = meta['meta_value']
#                    thumb = get_thumbnail(src,tid)
#                    ntid = store_thumbnail(dst,thumb)
 #                   meta['meta_value'] = ntid
  #                  for tmeta in get_meta(src,thumb['ID']):
   #                     tmeta['post_id'] = ntid
    #                    store_meta(dst,tmeta)
            
#            store_meta(dst,meta)

    dst.commit()


