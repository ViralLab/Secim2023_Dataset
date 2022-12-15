import os, sys
import gzip
import json
import argparse
from datetime import datetime
from tqdm import tqdm
import networkx as nx


def get_parser():
    parser = argparse.ArgumentParser(
        description="Export the GEFX file of the network of the given data"
    )
    parser.add_argument(
        "-p",
        "--path",
        help="the path to data folder",
        type=str,
    )
    parser.add_argument(
        "-apiv", "--apiv", help="Twitter API version - v1 or v2", type=int, default=1
    )

    parser.add_argument(
        "-net",
        "--net",
        help="Network style: 1.user-user 2.hashtag-hashtag 3.both ",
        type=int,
        default=3,
    )

    def valid_date(s):
        format = "%Y-%m-%d:%H:%M:%S"
        try:
            return datetime.strptime(s, format)
        except ValueError:
            msg = "not a valid date: {0!r} date format: {1}".format(s, format)
            raise argparse.ArgumentTypeError(msg)

    parser.add_argument(
        "-s",
        "--startdate",
        help="The Start Date - format YYYY-MM-DD:H:M:S  (Inclusive)",
        required=True,
        type=valid_date,
    )

    parser.add_argument(
        "-e",
        "--enddate",
        help="The End Date - format YYYY-MM-DD:%H:%M:%S (Inclusive)",
        required=True,
        type=valid_date,
    )
    return parser.parse_args()


def convert_str_to_datetime_v1(dtime):
    return datetime.strptime(dtime, "%a %b %d %H:%M:%S +0000 %Y")


def convert_str_to_datetime_v2(dtime):

    return datetime.fromisoformat(dtime[:-1])


def get_data_iterator(path, start_date, end_date, version=1):
    fnames = os.listdir(path)
    fnames = [f for f in fnames if f.endswith(".gz")]

    # filter out the files based on dates
    def select_between_datetime_range(fname):
        f_date = fname.split(".")[0][-10:]
        f_date = datetime.strptime(f_date, "%Y-%m-%d")
        if start_date.date() <= f_date.date() <= end_date.date():
            return True
        return False

    fnames = filter(select_between_datetime_range, fnames)
    fnames = sorted(
        fnames, key=lambda x: datetime.strptime(x.split(".")[0][-10:], "%Y-%m-%d")
    )

    for fname in fnames:
        with gzip.open(path + fname, "rb") as fh:
            for line in fh:
                tweet = json.loads(line)
                if version == 1:  # created_at

                    t_date = tweet["created_at"]
                    t_date = convert_str_to_datetime_v1(t_date)

                    if start_date <= t_date <= end_date:
                        yield tweet

                elif version == 2:  # data -> created_at
                    t_date = tweet["data"]["created_at"]
                    t_date = convert_str_to_datetime_v2(t_date)
                    if start_date <= t_date <= end_date:
                        yield tweet


def create_user_interaction_network(
    tweet_iterator, edge_selector=["retweet", "mention", "reply", "quote"], version=1
):
    """
    :return network
    """
    userProp = dict()
    net = nx.DiGraph()
    for tweet in tqdm(tweet_iterator):
        if version == 1:
            users = []
            tstamp = int(tweet["timestamp_ms"]) / 1000.0
            users.append(tweet["user"])
            uid = tweet["user"]["screen_name"].lower()
            if "retweet" in edge_selector:
                if "retweeted_status" in tweet:
                    users.append(tweet["retweeted_status"]["user"])
                    rid = tweet["retweeted_status"]["user"][
                        "screen_name"
                    ].lower()  # TODO

                    if net.has_edge(rid, uid):
                        net.edges[rid, uid]["weight"] += 1
                        net.edges[rid, uid]["retweet"] += 1
                    else:
                        net.add_edge(
                            rid, uid, weight=1, retweet=1, mention=0, reply=0, quote=0
                        )
            if "quote" in edge_selector:
                if "quoted_status" in tweet:
                    users.append(tweet["quoted_status"]["user"])

                    qid = tweet["quoted_status"]["user"]["screen_name"].lower()  # TODO
                    if net.has_edge(qid, uid):
                        net.edges[qid, uid]["weight"] += 1
                        net.edges[qid, uid]["quote"] += 1
                    else:
                        net.add_edge(
                            qid, uid, weight=1, retweet=0, mention=0, reply=0, quote=1
                        )
            if "mention" in edge_selector:
                if "retweeted_status" not in tweet:
                    for m in tweet["entities"]["user_mentions"]:
                        users.append(m)
                        mid = m["screen_name"].lower()  # TODO
                        if net.has_edge(uid, mid):
                            net.edges[uid, mid]["weight"] += 1
                            net.edges[uid, mid]["mention"] += 1
                        else:
                            net.add_edge(
                                uid,
                                mid,
                                weight=1,
                                mention=1,
                                reply=0,
                                retweet=0,
                                quote=0,
                            )
            if "reply" in edge_selector:
                if tweet["in_reply_to_user_id"] is not None and len(tweet["entities"]["user_mentions"]) > 0:
                    repid = tweet["entities"]["user_mentions"][0]["screen_name"].lower()
                    if net.has_edge(uid, repid):
                        net.edges[uid, mid]["weight"] += 1
                        net.edges[uid, mid]["reply"] += 1
                    else:
                        net.add_edge(
                            uid,
                            mid,
                            weight=0,
                            mention=0,
                            reply=1,
                            retweet=0,
                            quote=0,
                        )
            for u in users:
                uname = u["screen_name"].lower()
                if uname not in userProp:
                    userProp[uname] = dict()
                    userProp[uname]["id_str"] = u["id_str"]
                    userProp[uname]["tstamp_min"] = sys.maxsize
                    userProp[uname]["tstamp_max"] = 0

                for f in [
                    "id_str",
                    "screen_name",
                    "friends_count",
                    "followers_count",
                    "statuses_count",
                ]:
                    if u.get(f, None) != None:
                        userProp[uname][f] = u[f]

                userProp[uname]["tstamp_min"] = min(
                    userProp[uname]["tstamp_min"], tstamp
                )
                userProp[uname]["tstamp_max"] = max(
                    userProp[uname]["tstamp_max"], tstamp
                )

        elif version == 2:
            users = []

            tstamp = datetime.strptime(
                tweet["data"]["created_at"], "%Y-%m-%dT%H:%M:%S.%f%z"
            ).timestamp()
            users.append(tweet["includes"]["users"][0])
            uid = tweet["includes"]["users"][0]["username"].lower()

            if "retweet" in edge_selector:
                if (
                    "referenced_tweets" in tweet["data"]
                    and len(tweet["data"]["referenced_tweets"]) != 0
                    and tweet["data"]["referenced_tweets"][0]["type"] == "retweeted"
                ):
                    retweet_id = tweet["data"]["referenced_tweets"][0]["id"]

                    main_user_id = None
                    if "tweets" in tweet["includes"]:  # if retweeted tweet is available
                        for t in tweet["includes"]["tweets"]:
                            if t["id"] == retweet_id:
                                main_user_id = t["author_id"]
                                break
                        for u in tweet["includes"]["users"]:
                            if u["id"] == main_user_id:
                                users.append(u)
                                rid = u["username"].lower()  # TODO
                                break

                        if net.has_edge(rid, uid):
                            net.edges[rid, uid]["weight"] += 1
                            net.edges[rid, uid]["retweet"] += 1
                        else:
                            net.add_edge(
                                rid,
                                uid,
                                weight=1,
                                retweet=1,
                                mention=0,
                                reply=0,
                                quote=0,
                            )

            if "quote" in edge_selector:
                if (
                    "referenced_tweets" in tweet["data"]
                    and len(tweet["data"]["referenced_tweets"]) != 0
                    and tweet["data"]["referenced_tweets"][0]["type"] == "quoted"
                ):

                    quoted_id = tweet["data"]["referenced_tweets"][0]["id"]
                    main_user_id = None
                    if "tweets" in tweet["includes"]:  # if quoted tweet is available
                        for t in tweet["includes"]["tweets"]:
                            if t["id"] == quoted_id:
                                main_user_id = t["author_id"]
                                break
                        for u in tweet["includes"]["users"]:
                            if u["id"] == main_user_id:
                                users.append(u)
                                qid = u["username"].lower()  # TODO
                                break

                        if net.has_edge(qid, uid):
                            net.edges[qid, uid]["weight"] += 1
                            net.edges[qid, uid]["quote"] += 1
                        else:
                            net.add_edge(
                                qid,
                                uid,
                                weight=1,
                                retweet=0,
                                mention=0,
                                reply=0,
                                quote=1,
                            )

            if "reply" in edge_selector:
                if (
                    "referenced_tweets" in tweet["data"]
                    and len(tweet["data"]["referenced_tweets"]) != 0
                    and tweet["data"]["referenced_tweets"][0]["type"] == "replied_to"
                ):

                    replied_id = tweet["data"]["referenced_tweets"][0]["id"]
                    main_user_id = None
                    if "tweets" in tweet["includes"]:  # if replied tweet is available
                        for t in tweet["includes"]["tweets"]:
                            if t["id"] == replied_id:
                                main_user_id = t["author_id"]
                                break
                        for u in tweet["includes"]["users"]:
                            if u["id"] == main_user_id:
                                users.append(u)
                                qid = u["username"].lower()  # TODO
                                break

                        if net.has_edge(qid, uid):
                            net.edges[qid, uid]["weight"] += 1
                            net.edges[qid, uid]["reply"] += 1
                        else:
                            net.add_edge(
                                qid,
                                uid,
                                weight=1,
                                retweet=1,
                                mention=0,
                                reply=1,
                                quote=0,
                            )

            if "mention" in edge_selector:
                if "referenced_tweets" not in tweet["data"]:
                    if "mentions" in tweet["data"]["entities"]:
                        for m in tweet["data"]["entities"]["mentions"]:
                            users.append(m)
                            mid = m["username"].lower()  # TODO
                            if net.has_edge(uid, mid):
                                net.edges[uid, mid]["weight"] += 1
                                net.edges[uid, mid]["mention"] += 1
                            else:
                                net.add_edge(
                                    uid,
                                    mid,
                                    weight=1,
                                    mention=1,
                                    reply=0,
                                    retweet=0,
                                    quote=0,
                                )

            for u in users:
                uname = u["username"].lower()
                if uname not in userProp:
                    userProp[uname] = dict()
                    userProp[uname]["id_str"] = u["id"]
                    userProp[uname]["tstamp_min"] = sys.maxsize
                    userProp[uname]["tstamp_max"] = 0

                for f in ["id", "username"]:
                    if u.get(f, None) is not None:
                        userProp[uname][f] = u[f]
                for f in ["following_count", "followers_count", "tweet_count"]:
                    if u.get("public_metrics", None) is not None:
                        if u["public_metrics"].get(f, None) is not None:
                            if f == "following_count":
                                userProp[uname]["friends_count"] = u["public_metrics"][
                                    f
                                ]
                            else:
                                userProp[uname][f] = u["public_metrics"][f]

            userProp[uname]["tstamp_min"] = min(userProp[uname]["tstamp_min"], tstamp)
            userProp[uname]["tstamp_max"] = max(userProp[uname]["tstamp_max"], tstamp)

    return net


def create_hashtag_network(tweet_iterator, version=1):
    if version == 1:
        net = nx.Graph()
        htagCount, htagMinMaxTs = dict(), dict()
        
        counted= 0
        m  = 0
        for tweet in tqdm(tweet_iterator):
            if "retweeted_status" in tweet:
                m+=1 
                continue

            tstamp = int(tweet["timestamp_ms"]) / 1000.0

            htags = [h["text"].lower() for h in tweet["entities"]["hashtags"]]
           
            counted+=1
            for i in range(len(htags)):
                if htags[i] not in htagCount:
                    htagCount[htags[i]] = 0
                    htagMinMaxTs[htags[i]] = [sys.maxsize, 0]
                htagCount[htags[i]] += 1
                htagMinMaxTs[htags[i]][0] = min(htagMinMaxTs[htags[i]][0], tstamp)
                htagMinMaxTs[htags[i]][1] = max(htagMinMaxTs[htags[i]][1], tstamp)

                for j in range(i + 1, len(htags)):
                    if net.has_edge(htags[i], htags[j]):
                        net.edges[htags[i], htags[j]]["weight"] += 1
                    else:
                        net.add_edge(htags[i], htags[j], weight=1)

      
        print("N RT: ", m )
        print("Counted:",counted)
        values = sorted(list(htagCount.values()), reverse=True) 
        if len(values) > 250:
            topList = values[250]
        else: 
            topList = values[-1]
        for n in net.nodes():
            net.nodes[n]["count"] = htagCount.get(n, 0)
            net.nodes[n]["tstamp_min"] = htagMinMaxTs[n][0]
            net.nodes[n]["tstamp_max"] = htagMinMaxTs[n][1]
            if htagCount.get(n, 0) > topList:
                net.nodes[n]["name_viz"] = n
            else:
                net.nodes[n]["name_viz"] = ""

        return net
    elif version == 2:
        net = nx.Graph()
        htagCount, htagMinMaxTs = dict(), dict()
        for tweet in tqdm(tweet_iterator):
            if (
                "referenced_tweets" in tweet["data"]
                and len(tweet["data"]["referenced_tweets"]) != 0
                and tweet["data"]["referenced_tweets"][0]["type"] == "retweeted"
            ):
                continue

            tstamp = datetime.strptime(
                tweet["data"]["created_at"], "%Y-%m-%dT%H:%M:%S.%f%z"
            ).timestamp()

            if "hashtags" not in tweet["data"]["entities"]:
                continue

            htags = [h["tag"].lower() for h in tweet["data"]["entities"]["hashtags"]]

            for i in range(len(htags)):
                if htags[i] not in htagCount:
                    htagCount[htags[i]] = 0
                    htagMinMaxTs[htags[i]] = [sys.maxsize, 0]
                htagCount[htags[i]] += 1
                htagMinMaxTs[htags[i]][0] = min(htagMinMaxTs[htags[i]][0], tstamp)
                htagMinMaxTs[htags[i]][1] = max(htagMinMaxTs[htags[i]][1], tstamp)

                for j in range(i + 1, len(htags)):
                    if net.has_edge(htags[i], htags[j]):
                        net.edges[htags[i], htags[j]]["weight"] += 1
                    else:
                        net.add_edge(htags[i], htags[j], weight=1)
        values = sorted(list(htagCount.values()), reverse=True)
        if len(values) > 250:
            topList = values[250]
        else:
            topList = values[-1]
        for n in net.nodes():
            net.nodes[n]["count"] = htagCount.get(n, 0)
            net.nodes[n]["tstamp_min"] = htagMinMaxTs[n][0]
            net.nodes[n]["tstamp_max"] = htagMinMaxTs[n][1]
            if htagCount.get(n, 0) > topList:
                net.nodes[n]["name_viz"] = n
            else:
                net.nodes[n]["name_viz"] = ""

        return net


if __name__ == "__main__":
    args = get_parser()

    tweet_iterator = get_data_iterator(
        args.path, args.startdate, args.enddate, version=args.apiv
    )

    if args.net == 3:
        user_user_net = create_user_interaction_network(
            tweet_iterator, version=args.apiv
        )
        tweet_iterator = get_data_iterator(
            args.path, args.startdate, args.enddate, version=args.apiv
        )
        hashtag_net = create_hashtag_network(tweet_iterator, version=args.apiv)
        print("SAVING!")
        nx.write_gexf(user_user_net, "user_user_net.gexf")
        nx.write_gexf(hashtag_net, "hashtag_net.gexf")
    elif args.net == 2:
        hashtag_net = create_hashtag_network(tweet_iterator, version=args.apiv)
        print("SAVING!")
        nx.write_gexf(hashtag_net, "hashtag_net.gexf")
    elif args.net == 1:
        user_user_net = create_user_interaction_network(
            tweet_iterator, version=args.apiv
        )
        print("SAVING!")
        nx.write_gexf(user_user_net, "user_user_net.gexf")
