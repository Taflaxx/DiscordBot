import typing

# bosses ordered by raid wing
bosses = typing.Literal[
         "Vale Guardian", "Gorseval the Multifarious", "Sabetha the Saboteur",
         "Slothasor", "Matthias Gabrel",
         "Keep Construct", "Xera",
         "Cairn the Indomitable", "Mursaat Overseer", "Samarog", "Deimos",
         "Soulless Horror", "Dhuum",
         "Conjured Amalgamate", "Twin Largos", "Qadim",
         "Cardinal Sabir", "Cardinal Adina", "Qadim the Peerless",
         # Strikes
         "Captain Mai Trin", "Ankka", "Minister Li", "The Dragonvoid"
]

# Discord bot activity
activity = typing.Literal[
    "Playing", "Listening", "Watching"
]