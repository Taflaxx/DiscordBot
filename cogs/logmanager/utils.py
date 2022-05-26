from collections import Counter
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
from datetime import datetime, timezone

# Boss name abbreviations for easier searching
boss_abrv = {"sab": "Sabetha the Saboteur", "gors": "Gorseval the Multifarious", "vg": "Vale Guardian",
             "matt": "Matthias Gabrel", "sloth": "Slothasor", "kc": "Keep Construct",
             "mo": "Mursaat Overseer", "sam": "Samarog", "sama": "Samarog", "dei": "Deimos", "sh": "Soulless Horror",
             "tl": "Twin Largos", "ca": "Conjured Amalgamate", "qpeer": "Qadim the Peerless",
             "q1": "Qadim", "q2": "Qadim the Peerless", "qtp": "Qadim the Peerless", "sabir": "Cardinal Sabir",
             "adina": "Cardinal Adina"}


rank_emojis = {1: ":first_place:", 2: ":second_place:", 3: ":third_place:"}


def most_frequent_embed(list, limit=5):
    counter = Counter(list).most_common()
    ret = ""
    for i in range(0, limit):
        ret += f"{counter[i][0][0]}: {counter[i][1]}\n"
    return ret


def sort_dict(dictionary):
    return sorted(dictionary.items(), key=lambda x:x[1])


def plot_lineplot(data, title, hue=None, format_percent=False):
    # Plot
    sns.set_style("darkgrid")
    sns_plot = sns.lineplot(data=data, x=data.columns[0], y=data.columns[1], hue=hue).set_title(title)
    if format_percent:
        plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())
    plt.xticks(rotation=25)
    plt.tight_layout()
    # Save plot to file
    filename = f"{datetime.now(tz=timezone.utc).strftime('plot-%Y%m%d-%H%M%S')}.png"
    filepath = f"cogs/logmanager/tmp/{filename}"
    sns_plot.figure.savefig(filepath)
    # Clear the figure to stop them from stacking on top of each other
    plt.clf()
    return filepath, filename


# Splits log text into multiple embed fields under one title
def split_embed(embed, title, text):
    text = text.splitlines(keepends=True)
    text_short = ""
    for line in text:
        if (len(text_short) + len(line)) <= 1024:
            text_short += line
        else:
            embed.add_field(name=title, value=text_short, inline=False)
            text_short = line
            title = "\u200b"    # Invisible character to not repeat title

    if text_short != "":
        embed.add_field(name=title, value=text_short, inline=False)

    return embed


# Timedelta to string
def strfdelta(tdelta):
    m, s = divmod(tdelta.seconds, 60)
    ms = f"{tdelta.microseconds:06d}"[:3]
    return f"{m:02d}m {s:02d}s {ms} ms"