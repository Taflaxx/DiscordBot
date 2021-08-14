from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timezone

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
    return sorted(dictionary.items(), key=lambda x:x[1])


def plot_lineplot(data, title):
    # Plot
    sns.set_style("darkgrid")
    sns_plot = sns.lineplot(data=data, x=data.columns[0], y=data.columns[1]).set_title(title)
    plt.xticks(rotation=25)
    plt.tight_layout()
    # Save plot to file
    filename = f"{datetime.now(tz=timezone.utc).strftime('plot-%Y%m%d-%H%M%S')}.png"
    filepath = f"cogs/logmanager/tmp/{filename}"
    sns_plot.figure.savefig(filepath)
    # Clear the figure to stop them from stacking on top of each other
    plt.clf()
    return filepath, filename
