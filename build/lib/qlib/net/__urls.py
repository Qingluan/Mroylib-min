def get_domain(main_url, head='http'):
    if '/' in main_url:
        if main_url.startswith("http"):
            main_url = head + '://' + main_url.split("/")[2]
        else:
            main_url = head + '://'+ main_url.split("/")[0]
    else:
        if not main_url.startswith("http"):
            main_url = head + '://'+ main_url.split("/")[0]
    return main_url



def gen_urls(f, default="http://"):
    with open(f) as fp:
        for line in fp:
            t = check_uri(line, default)
            if t:
                yield t


def check_uri(line, default="http://"):
    l = line.strip()
    if len(l.split()) != 1:
        return False

    if not l.startswith(default):
        return default + l
    else:
        return l

def url_to_db(db, url):
    rs = re.findall(r'(?:http\://.+?)(/.+)', url)
    if rs:
        db.insert("urls",**{"uri":rs[0]})
