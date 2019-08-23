import requests
import discord
from discord import Webhook, RequestsWebhookAdapter, Embed
import config as cfg
import asyncio
import aiomysql
import datetime
import time

# Create webhook
webhook = Webhook.partial(cfg.WEBHOOK_ID, cfg.WEBHOOK_TOKEN,
    adapter=RequestsWebhookAdapter())


async def postStats():
    try:
        dzconn = await aiomysql.connect(host=cfg.dzhost, port=cfg.dzport, user=cfg.dzuser, password=cfg.dzpass, db=cfg.dzschema, autocommit=True)
        dzcur = await dzconn.cursor(aiomysql.DictCursor)
        accountSums = {"account": ["kills", "score", "deaths", "locker", "total_connections"]}
        playerSums = {"player": ["money"]}

        playerAvg = playerSums
        accountAvg = accountSums
        
        contructionCount = {"construction": ["*"]}
        loadoutCount = {"loadout": ["*"]}
        clanCount = {"clan": ["*"]}
        clanMarkerCount = {"clan_map_marker": ["*"]}
        territoryCount = {"territory": ["*"]}
        vehicleCount = {"vehicle": ["*"]}


        tables = {"Totals": [accountSums, playerSums], "Averages": [
            accountAvg, playerAvg], "Counts": [contructionCount, loadoutCount, clanCount, clanMarkerCount, territoryCount, vehicleCount]}
        sums = {}
        averages = {}
        counts = {}
        realNames = {"kills": "Kills", "deaths": "Deaths", "score": "Respect", "money": "Poptabs On Character", "locker": "Locker Poptabs", "construction": "Objects", "loadout": "Loadouts", "clan": "Clans", "clan_map_marker": "Clan Map Markers", "territory": "Territories", "vehicle": "Vehicles", "total_connections": "Total Connections"} 

        for type in tables:
            for x in tables[type]:
                for key in x:
                    for column in x[key]:
                        if type == "Totals":
                            query = f"SELECT SUM({column}) AS Total FROM {key};"
                            await dzcur.execute(query)
                            result = await asyncio.gather(dzcur.fetchone())
                            result = result[0]['Total']
                            sums[column] = result
                        elif type == "Averages":
                            query = f"SELECT AVG({column}) AS Average FROM {key};"
                            await dzcur.execute(query)
                            result = await asyncio.gather(dzcur.fetchone())
                            result = result[0]['Average']
                            result = round(result, 0)
                            averages[column] = result
                        elif type == "Counts":
                            if column == "*":
                                query = f"SELECT COUNT({column}) AS Total FROM {key};"
                            await dzcur.execute(query)
                            result = await asyncio.gather(dzcur.fetchone())
                            result = result[0]['Total']
                            result = round(result, 2)
                            counts[key] = result
        sumStr = ""
        avgStr = ""
        countStr = ""

        
        for key, value in sums.items():
            trueName = realNames[key]
            sumStr += f"**{trueName}** | **{value}**\n"
        
        for key, value in averages.items():
            trueName = realNames[key]
            avgStr += f"**{trueName}** | **{value}**\n"
        
        for key, value in counts.items():
            trueName = realNames[key]
            countStr += f"**{trueName}** | **{value}**\n"

        embed = discord.Embed(
            title=f"Stats at **{time.strftime('%H:%M')} {time.tzname[0]}**", colour=discord.Colour(0x32CD32))
        embed.set_footer(text="TW Exile Stats | twist#7777")
        embed.add_field(name=f"Totals:",
                        value=sumStr +  countStr, inline=False)
        embed.add_field(name=f"Averages:",
                        value=avgStr, inline=False)
                
        webhook.send(embed=embed)
    except Exception as e:
        print(e)
    finally:
        dzconn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(postStats())
