node = {
    "node_data": {
        "msg": {
            "action": "add_node",
            "nodes": [
            ]
        }
    },
    "link_data": {
        "msg": {
            "action": "add_link",
            "links": [
            ]
        }
    },
    "node": {
        "id": None,
        "attr": "Unknow",
    },
    "link": {
        "from": None,
        "to": None,
        "value": 60,
    },

    "must": [
        "id", "from", "to"
    ]

}

geo = {
    "msg": {
        "msg":{
            "action": "geo",
            "title": None,
            "content": None,
            "type": None,
            "place": None,
        }
    },
    "control":{
        "msg":{
            "action":"move_to",
            "place": None,
        }
    },
    "zoom":{
        "msg":{
            "action":"zoom",
            "zoom": None,
        }
    },
    "mark":{
        "msg":{
            "action": "mark",
            "coor": None,
            "msg": None
        }
    },
    "sky_line":{
        "msg":{
            "action": "fly_link",
            "from": None,
            "to": None,
            "time": 60,
        }
    },

}
