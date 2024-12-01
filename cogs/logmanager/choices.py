import typing

# Discord bot activity
activity = typing.Literal[
    "Playing", "Listening", "Watching"
]

# Log Order
log_order = typing.Literal[
    "Target DPS",
    "Breakbar",
    "Damage taken",
    "Date",
    "Duration"
]
