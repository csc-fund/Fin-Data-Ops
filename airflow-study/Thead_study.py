import concurrent.futures
import urllib.request

URLS = ['http://www.foxnews.com/',
        'http://www.cnn.com/',
        'http://europe.wsj.com/',
        'http://www.bbc.co.uk/',
        'http://some-made-up-domain.com/']


# Retrieve a single page and report the URL and contents
def load_url(url, timeout):
    # with urllib.request.urlopen(url, timeout=timeout) as conn:
    #     cont = conn.read()
    # print(url[:10])
    if url[:10] == 'http://som':
        raise Exception
    # return 1


# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]  # 执行的表名
        try:
            res = future.result()  # 线程执行的结果 ,没有返回就是None
        except Exception as exc:
            # 异常执行
            print('%r generated an exception: %s' % (url, exc))
        else:
            # 正常执行
            print('%r ok' % url)
