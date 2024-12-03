import requests
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine


def str_to_int(x):
    if x != None:
        try:
            int_value = int(x)
            return int_value
        except ValueError:
            return None
    else:
        return None
    
def comma_separate(li,x):
    ret = ''

    for i in li:
        type = i.get(x)
        if ret == '':
            ret = type
        else:
            ret = ret+','+type

    return ret

def calculate_Discount(totalAmount,discountAmount):
    print(totalAmount)
    print(discountAmount)
    if totalAmount!= None and totalAmount!= 0 and discountAmount != None :
        _discountAmount =  discountAmount/1000000
        return (_discountAmount/totalAmount)*100
    else:
        return None
        

def get_or_create_author(cursor,author_name):
    cursor.execute(
        """
            SELECT authorID FROM author WHERE authorName = ?
        """, (author_name)
        )
    result = cursor.fetchone()
    
    
    if result:
        print('auth',result[0])
        return result[0]
    else:
        p = """
            INSERT INTO author(authorName)
            VALUES(?)
        """
        q = (author_name)
        print('q',q)
        cursor.execute(p,q)
        return cursor.lastrowid
    
def get_or_create_category(cursor,author_name):
    cursor.execute(
        """
            SELECT categoryID FROM author WHERE categoryName = ?
        """, (author_name)
        )
    result = cursor.fetchone()
    
    
    if result:
        return result[0]
    else:
        p = """
            INSERT INTO category(categoryName)
            VALUES(?)
        """
        q = (author_name)
        cursor.execute(p,q)
        
        return cursor.lastrowid

def get_nested(data, keys, default=None):
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data.get(key, default)
        else:
            return default
    return data

def fetch_books(api_url,query,api_key, max_records=500, page_size=40):
    all_data = []  
    start_index = 0  
    
    while start_index < max_records:
        params = {
            "q": query,
            "key": api_key,
            "maxResults":40,
            "startIndex":start_index
        }
        
        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])

            for item in items:
                volume_info = item.get('volumeInfo', {})
                accessInfo = item.get('accessInfo', {})
                saleInfo = item.get('saleInfo', {})

                offers = saleInfo.get("offers", [])

                p_obj = None
                for listPriceobj in offers:
                    p_obj = listPriceobj.get('listPrice', {}).get('amountInMicros')
                    break

                r_obj = None
                for retailPriceobj in offers:
                    r_obj = retailPriceobj.get('retailPrice     ', {}).get('amountInMicros')
                    break


                print(get_nested(volume_info.get("imageLinks", {}), ["thumbnail"]))

                all_data.append({
                    'bookID': item.get('id', 'N/A'),
                    'search_key':query,
                    'book_title': volume_info.get('title', 'N/A'),
                    'book_subtitle': volume_info.get('subtitle', 'N/A'),
                    'Authors': volume_info.get('authors', []),
                    'publisher': volume_info.get('publisher', '-'),
                    'book_description': volume_info.get('description', 'N/A'),
                    'industryIdentifiers':comma_separate(volume_info.get("industryIdentifiers", []),"type"),
                    'text_readingModes':volume_info.get('readingModes',{}).get('text',False),
                    'image_readingModes':volume_info.get('readingModes',{}).get('image',False),
                    'pageCount': volume_info.get('pageCount', 0),
                    'categories': volume_info.get('categories', []),
                    'language': volume_info.get('language', 'N/A'),

                    'imageLinks': get_nested(volume_info.get("imageLinks", {}), ["thumbnail"]),
                    # 'imageLinks':volume_info.get('imageLinks',{}).get('thumbnail','N/A'),
                    'ratingsCount': volume_info.get('ratingsCount', 0),
                    'averageRating': volume_info.get('averageRating', 0),
                    'country': accessInfo.get('country', '-'),
                    'saleability': saleInfo.get('saleability', '-'),
                    'isEbook': saleInfo.get('isEbook', False),
                    'amount_listPrice': saleInfo.get('listPrice', {}).get('amount', 0),
                    'currencyCode_listPrice': saleInfo.get('listPrice', {}).get('currencyCode', '-'),
                    'amount_retailPrice': saleInfo.get('retailPrice', {}).get('amount', 0),
                    'currencyCode_retailPrice': saleInfo.get('retailPrice', {}).get('currencyCode', '-'),
                    'buyLink': saleInfo.get('buyLink', 'N/A'),
                    'year': (str_to_int(volume_info.get('publishedDate')[:4])) if volume_info.get('publishedDate') is not None else None,
                    'listPricediscount': calculate_Discount(saleInfo.get('listPrice', {}).get('amount'),discountAmount=p_obj),
                    'retailPricediscount': calculate_Discount(saleInfo.get('retailPrice', {}).get('amount'),discountAmount=r_obj)
                })   

            start_index = start_index+page_size

        else:
            print(f"Error: Unable to fetch data. Status Code: {response.status_code}")
            break

    return pd.DataFrame(all_data)


def save_to_db(df,connectionString):
    engine = create_engine(connectionString)
    # connection = connectionString.cursor()

    try:

        authordf = []
        catdf = []
        for _,row in df.iterrows():
            for author in row['Authors']:
                authordf.append({
                    'author': author,
                    'bookID':row['bookID']
                })
            
            for cat in row['categories']:
                catdf.append({
                    'author': cat,
                    'bookID':row['bookID']
                })

        print(authordf)

        a = pd.DataFrame(authordf)
        c = pd.DataFrame(catdf)

        a.to_sql('NewAuthor', con=engine, if_exists='append', index=False)
        
        c.to_sql('NewCat', con=engine, if_exists='append', index=False)
        df.drop(columns=['Authors'], inplace=True)
        df.drop(columns=['categories'], inplace=True)


        df.to_sql('NewBooks', con=engine, if_exists='append', index=False)

        return True
    except Exception as e:
        print(e)
        return False
    
    

engine = create_engine('mysql+pymysql://root:Password@127.0.0.1:3306/guvi_bookshelf')


url = "https://www.googleapis.com/books/v1/volumes"

search = st.text_input("Enter Keyword to fetch:")

if search and st.button("Search"):
    bookdf = fetch_books(url,query=search,api_key='API Key')
    db_str = "mysql+pymysql://root:Password@127.0.0.1:3306/guvi_bookshelf"
    save_to_db(df=bookdf,connectionString=db_str)




# st.write('yes' if save_to_db else 'no')

if save_to_db:

    
    options = ['Q1','Q2','Q3','Q4','Q5','Q6','Q7','Q8','Q9','Q10','Q11','Q12','Q13','Q14','Q15','Q16','Q17','Q18','Q19','Q20']

    selectedOption = st.selectbox("Choose question: ",options)

    if selectedOption == 'Q1':
        
        q1 = f"""
        SELECT 
            CASE 
                WHEN isEbook = TRUE THEN 'eBook' 
                ELSE 'Physical' 
            END AS format, 
            COUNT(*) AS count
        FROM NewBooks
        WHERE search_key like'%%{search}%%'
        GROUP BY format;
        """
        books_df = pd.read_sql(q1, engine)

        st.title("Check Availability of eBooks vs Physical Books")

        st.dataframe(books_df)

        st.bar_chart(books_df.set_index("format"))


    elif selectedOption == 'Q2':
        st.write(search)
        q2 = f"""
        SELECT 
            publisher ,
            COUNT(*) AS book_count
        FROM NewBooks
        WHERE publisher != '-' AND search_key like '%%{search}%%'
        GROUP BY publisher
        ORDER BY book_count DESC
        LIMIT 1
        """
        resulttoppublisher_df = pd.read_sql(q2, engine)

        st.title("Publisher with the Most Books Published")

        if not resulttoppublisher_df.empty:
            toppublisher_df = resulttoppublisher_df.iloc[0]
            st.write("Top Publisher: "+toppublisher_df['publisher'])
            st.write("Number of Books Published: "+ str(toppublisher_df['book_count']))
        else:
            st.write("No Data found")


    elif selectedOption == 'Q3':
        q3 = f"""
            SELECT 
            publisher,
            AVG(ratingsCount) AS ratingsCount_avg
        FROM NewBooks
        WHERE publisher != '-' AND search_key like '%%{search}%%'
        GROUP BY publisher
        ORDER BY ratingsCount_avg DESC;
        """
        avgrating_df = pd.read_sql(q3, engine)

        if not avgrating_df.empty:
            st.title("Publisher with the Highest Average Rating")
            avg_df = avgrating_df.iloc[0]
            st.write("Publisher: "+avg_df['publisher'])
            st.write("Average rating "+ str(avg_df['ratingsCount_avg']))
        else:
            st.write("No Data found")


    elif selectedOption == 'Q4':
        q4 = f"""
            SELECT
            book_title,
            amount_retailPrice
        FROM NewBooks
        WHERE search_key like '%%{search}%%'
        ORDER BY amount_retailPrice DESC
        LIMIT 5
        """
        top5_df = pd.read_sql(q4, engine)

        if not top5_df.empty:
            st.title("Top 5 Most Expensive Books by Retail Price")
            st.dataframe(top5_df)
        else:
            st.write("No Data found")

    elif selectedOption == 'Q5':
        q5 = f"""
        SELECT DISTINCT
            book_title,
            year,
            pageCount
        FROM NewBooks
        WHERE year > 2010
        AND pageCount >= 500 AND search_key like '%%{search}%%'
        ORDER BY pageCount DESC;
        """

        q5_df = pd.read_sql(q5, engine)

        if not q5_df.empty:
            st.title("Books Published After 2010 with at Least 500 Pages")
            st.write("Count: "+ str(len(q5_df)))
            st.dataframe(q5_df)
        else:
            st.write("No Data found")

    elif selectedOption == 'Q6':
        q6 = f"""
        SELECT DISTINCT
            book_title,
            retailPricediscount
        FROM NewBooks
        WHERE retailPricediscount > 20 AND search_key like '%%{search}%%'
        ORDER BY retailPricediscount DESC;
        """

        q6_df = pd.read_sql(q6, engine)

        if not q6_df.empty:
            st.title("Books with Discounts Greater than 20%")
            st.write("Count: "+ str(len(q6_df)))
            st.dataframe(q6_df)
        else:
            st.write("No Data found")

    elif selectedOption == 'Q7':
        q7 = f"""
            SELECT 
            CASE 
                WHEN isEbook = TRUE THEN 'eBook' 
                ELSE 'Physical' 
            END AS format, 
            AVG(pageCount) AS count
        FROM NewBooks
        WHERE search_key like '%%{search}%%'
        GROUP BY format;
        """
        q7_df = pd.read_sql(q7, engine)

        if not q7_df.empty:
            st.title("Average Page Count for eBooks vs Physical Books")
            st.dataframe(q7_df)
            st.bar_chart(q7_df.set_index("format"))
        else:
            st.write("No Data found")

    elif selectedOption == 'Q8':
        q8 = f"""
            SELECT 
            author, 
            count(bookID) AS count
        FROM NewAuthor
        GROUP BY author
        ORDER BY count
        LIMIT 3;
        """
        q8_df = pd.read_sql(q8, engine)

        if not q8_df.empty:
            st.title("Top 3 Authors with the Most Books")
            st.dataframe(q8_df)
            st.bar_chart(q8_df.set_index("author"))
        else:
            st.write("No Data found")

    elif selectedOption == 'Q9':
        q9 = f"""
            SELECT 
            publisher, 
            COUNT(bookID) AS count
        FROM NewBooks
        WHERE publisher != '-' AND search_key like '%%{search}%%'
        GROUP BY publisher HAVING COUNT(bookID)>10
        ORDER BY count DESC;
        """
        q9_df = pd.read_sql(q9, engine)

        if not q9_df.empty:
            st.title("Publishers with More than 10 Books")
            st.dataframe(q9_df)
            st.bar_chart(q9_df.set_index("publisher"))
        else:
            st.write("No Data found")

    elif selectedOption == 'Q10':
        q10 = f"""
        SELECT
            c.author as Catagory,
            AVG(b.pageCount) AS Count
        FROM
            NewCat c
        JOIN
            NewBooks b ON c.bookID = b.bookID
        WHERE b.search_key like '%%{search}%%'
        GROUP BY
            c.author
        ORDER BY
            Count DESC;
        """
        q10_df = pd.read_sql(q10, engine)

        if not q10_df.empty:
            st.title("Average Page Count for Each Category")
            st.dataframe(q10_df)
            st.bar_chart(q10_df.set_index("Catagory"))
        else:
            st.write("No Data found")


    elif selectedOption == 'Q11':
        q11= f"""
            SELECT
                b.book_title,
                COUNT(a.author) AS Count
            FROM
                NewBooks b
            JOIN
                NewAuthor a ON b.bookID = a.bookID
            WHERE b.search_key like '%%{search}%%'
            GROUP BY b.book_title HAVING COUNT(a.author) > 3
            ORDER BY Count DESC;
        """
        q11_df = pd.read_sql(q11, engine)

        if not q11_df.empty:
            st.title("Average Page Count for Each Category")
            st.dataframe(q11_df)
            st.bar_chart(q11_df.set_index("book_title"))
        else:
            st.write("No Data found")

    elif selectedOption == 'Q12':
        q12= f"""
        SELECT 
            book_title,
            ratingsCount
        FROM 
            NewBooks
        WHERE 
            RatingsCount > (SELECT AVG(ratingsCount) FROM NewBooks) AND search_key like '%%{search}%%'
        ORDER BY ratingsCount DESC;
        """

        q12_1= f"""SELECT AVG(ratingsCount) as a FROM NewBooks WHERE search_key like '%%{search}%%';"""

        q12_df = pd.read_sql(q12, engine)
        q12_1_df = pd.read_sql(q12_1, engine)

        if not q12_df.empty:
            st.title("Books with Ratings Count Greater Than the Average")
            avgrat_df = q12_1_df.iloc[0]
            st.write("Average Rating: "+str(avgrat_df["a"]))
            st.dataframe(q12_df)
            st.bar_chart(q12_df.set_index("book_title"))
        else:
            st.write("No Data found")

    elif selectedOption == 'Q13':
        q13 = f"""
        SELECT 
            a.author,
            b.year,
            GROUP_CONCAT(DISTINCT b.book_title) AS Books
        FROM 
            NewAuthor a
        JOIN 
            NewBooks b ON a.bookID = b.bookID
        WHERE b.search_key like '%%{search}%%'
        GROUP BY 
            a.author, b.year
        HAVING 
            COUNT(b.bookID) > 1;
        """
        q13_df = pd.read_sql(q13, engine)

        if not q13_df.empty:
            st.title("Books with the Same Author Published in the Same Year")
            st.dataframe(q13_df)
        else:
            st.write("No Data found")

    elif selectedOption == 'Q14':
        st.title("Books with a Specific Keyword in the Title")

        input = st.text_input("Enter keyword to search in book titles:")

        if input and st.button("ckeck"):
            q14 = f"""SELECT book_title FROM NewBooks WHERE book_title LIKE '%%{input}%%' AND search_key like '%%{search}%%'"""
            st.write(q14)
            q14_df = pd.read_sql(q14, engine)
            st.write(q14_df)
            if len(q14_df) >0:
                st.write("No. of Books found: "+str(len(q14_df)))   
                st.dataframe(q14_df)
            else:
                st.write("No Data found")

        else:
            st.write("Input is empty")



    elif selectedOption == 'Q15':
        priceq15 = f"""
        SELECT 
            year,
            AVG(amount_listPrice) AS Price
        FROM 
            NewBooks
        WHERE search_key like '%%{search}%%'
        GROUP BY 
            year
        ORDER BY 
            Price DESC
        LIMIT 1;
        """

        retailq15 = f"""
        SELECT 
            year,
            AVG(amount_retailPrice) AS Retail_Price
        FROM 
            NewBooks
        WHERE search_key like '%%{search}%%'
        GROUP BY 
            year
        ORDER BY 
            Retail_Price DESC
        LIMIT 1;
        """

        st.title("Year with the Highest Average Book Price")

        priceq15_df = pd.read_sql(priceq15, engine)
        price_df = priceq15_df.iloc[0]
        st.write("Year: "+str(price_df["year"]))
        st.write("Price: "+str(price_df["Price"]))

        retailq15_df = pd.read_sql(retailq15, engine)
        retail_df = retailq15_df.iloc[0]
        st.write("Year Retail: "+str(retail_df["year"]))
        st.write("Retail Price: "+str(retail_df["Retail_Price"]))

    elif selectedOption == 'Q16':
        q16=f"""
        SELECT COUNT(DISTINCT na.author) AS count
        FROM NewAuthor na
        JOIN NewBooks nb1 ON na.bookID = nb1.bookID
        JOIN NewBooks nb2 ON na.bookID = nb2.bookID AND nb1.year = nb2.year - 1
        JOIN NewBooks nb3 ON na.bookID = nb3.bookID AND nb2.year = nb3.year - 1
        WHERE nb1.search_key like '%%{search}%%';
        """

        q16_df = pd.read_sql(q16, engine)

        if not q16_df.empty:
            st.title("Count Authors Who Published 3 Consecutive Years")
            auth_count = q16_df.iloc[0]
            st.write("Count: "+str(auth_count["count"]))
        else:
            st.write("No Data found")

    elif selectedOption == 'Q17':
        q17 = f"""
        SELECT 
            a.author,
            b.year,
            COUNT(b.bookID) AS book_count
        FROM 
            NewBooks b
        JOIN 
            NewAuthor a ON b.bookID = a.bookID
        WHERE
            b.publisher != '-' AND b.search_key like '%%{search}%%'
        GROUP BY 
            a.author, b.year
        HAVING 
            COUNT(DISTINCT b.publisher) > 1;
        """

        st.title("SQL query to find authors who have published books in the same year but under different publishers.")
        q17_df = pd.read_sql(q17, engine)
        st.dataframe(q17_df)


    elif selectedOption == 'Q18':
        q18=f"""
        SELECT 
            AVG(CASE WHEN isEbook = true THEN amount_retailPrice END) AS avg_ebook_price,
            AVG(CASE WHEN isEbook = false THEN amount_retailPrice END) AS avg_physical_price
        FROM 
            NewBooks
        WHERE search_key like '%%{search}%%';
        """
        st.title("SQL query to find authors who have published books in the same year but under different publishers.")
        q18_df = pd.read_sql(q18, engine)
        st.dataframe(q18_df)


    elif selectedOption == 'Q19':
        q19=f"""
        WITH func AS (
            SELECT 
                AVG(ratingsCount) AS avg_rating,
                STDDEV(ratingsCount) AS std_dev
            FROM newbooks
            WHERE ratingsCount IS NOT NULL
        )
        SELECT 
            book_title,
            averageRating,
            ratingsCount
        FROM newbooks
        WHERE ratingsCount IS NOT NULL
        AND (
            ratingsCount > (SELECT avg_rating + 2 * std_dev FROM func)
            OR ratingsCount < (SELECT avg_rating - 2 * std_dev FROM func)
        ) AND search_key like '%%{search}%%';
        """

        q19_df = pd.read_sql(q19, engine)

        if not q19_df.empty:
            st.title("Write a SQL query to identify books that have an averageRating that is more than two standard deviations away from the average rating of all books. Return the title, averageRating, and ratingsCount for these outliers")
            st.dataframe(q19_df)
        else:
            st.write("No Data found")


    else:
        q20=f"""
        WITH func AS (
        SELECT 
            publisher,
            AVG(ratingsCount) AS average_rating,
            COUNT(*) AS book_count
        FROM NewBooks
        WHERE ratingsCount IS NOT NULL AND search_key LIKE '%%{search}%%'
        GROUP BY publisher
        HAVING COUNT(*) > 10
    )
    SELECT 
        publisher,
        average_rating,
        book_count
    FROM func
    WHERE average_rating = (SELECT MAX(average_rating) FROM func);
        """

        q20_df = pd.read_sql(q20, engine)

        if not q20_df.empty:
            st.title("Create a SQL query that determines which publisher has the highest average rating among its books, but only for publishers that have published more than 10 books. Return the publisher, average_rating, and the number of books published.")
            st.dataframe(q20_df)
        else:
            st.write("No Data found")
else:
    st.write("No Data found")