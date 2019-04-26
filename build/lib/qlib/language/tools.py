import re


def extract(string, pre='(', back=')'):
    """
    Can extract content between from spic[0] and spic[1], default spic is '()' , which from string.
    """

    l_count = 0
    while string.find(pre) != -1:
        l_count += 1
        s = string.find(pre) + len(pre)
        string = string[s:]

        last_b = string.rfind(back)
        next_s = string.find(pre)
        next_b = string.find(back)
        if next_b < next_s:
            yield string[:next_b]
            string = string[next_b:] 
        else:
            yield string[:last_b]
            string = string[:last_b]
        if string is None:
            break


def get_from_func(string, name):

    while string.find(name) != -1:
        string = string[string.find(name) + len(name):]
        yield next(extract(string))
        if not string:
            break


def get_from_tag(string, tag):
    if isinstance(string, bytes):
        string = string.decode("utf8")
    g_r = re.compile(r'(?:\<{tag}.*?\>)(.+?)(?:</{tag}>)'.format(tag=tag))
    if string is None:
        return

    for item in re.findall(g_r, string):
        yield item
    # pre='<%s>' % tag
    # back = '</%s>' % tag
    # if string is None:
    #     return ''
    # while string.find(tag) != -1:
    #     yield next(extract(string, pre=pre, back=back))
    #     string = string[string.find(pre) + len(pre):]
    #     if string is None:
    #         break


def replace_tag_to(string, func=False, **tags):
    """
    relace tag's content to value in tags

    @func = False/True
        while set True , will use tags's value should pass callback function, not value

    """
    

    def _tag(tag):
        return '<%s>' % tag, '<%s/>' % tag

    for tag in tags:
        val = tags[tag]
        replace_r = r'\<{tag}.*?\>.+?\<\/{tag}\>'.format(tag=tag)
        for raw in get_from_tag(string, tag):
            if not func:
                # string = string.replace('<{tag}>{raw}</{tag}>'.format(tag=tag, raw=raw), val)
                string = re.sub(replace_r, val, string)
            else:
                string = re.sub(replace_r, val(raw), string)
                # string = string.replace('<{tag}>{raw}</{tag}>'.format(tag=tag, raw=raw), val(raw))

    return string