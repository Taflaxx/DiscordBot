from collections import Counter

# Boss name abbreviations for easier searching
boss_abrv = {"sab": "Sabetha the Saboteur", "gors": "Gorseval the Multifarious", "vg": "Vale Guardian",
             "matt": "Matthias Gabrel", "sloth": "Slothasor", "kc": "Keep Construct",
             "mo": "Mursaat Overseer", "sam": "Samarog", "sama": "Samarog", "dei": "Deimos", "sh": "Soulless Horror",
             "tl": "Twin Largos", "ca": "Conjured Amalgamate", "qpeer": "Qadim the Peerless",
             "q1": "Qadim", "q2": "Qadim the Peerless", "qtp": "Qadim the Peerless", "sabir": "Cardinal Sabir",
             "adina": "Cardinal Adina"}


def most_frequent_embed(list, limit=5):
    counter = Counter(list).most_common()
    ret = ""
    for i in range(0, limit):
        ret += f"{counter[i][0][0]}: {counter[i][1]}\n"
    return ret


def sort_dict(dictionary):
    return sorted(dictionary.items(), key=lambda x:x[1])#{k: v for k, v in sorted(dictionary.items(), key=lambda item: item[1])}