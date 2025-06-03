from datetime import datetime, timedelta, time

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import pytz
import seaborn as sns

import config
from utils.mongo_controller import mongo_controller

# Define the La Paz timezone
la_paz_timezone = pytz.timezone('America/La_Paz')


def line_graph_x_days_price(timestamp, fiat, days=1):
    """
    Generate a line graph for the price data of a given cryptocurrency and fiat currency pair over a specified number of days,
    collapsing overnight gaps but preserving scaled distances within active hours (7:00 to 23:59 each day).

    Args:
        timestamp (datetime): The timestamp for the data query.
        fiat (str): The fiat currency to query.
        days (int, optional): The number of days to query. Defaults to 1.

    Returns:
        Path to the saved plot image.
    """
    # Assign UTC timezone and convert to target timezone
    timestamp_lp = timestamp.replace(tzinfo=pytz.utc).astimezone(la_paz_timezone)

    # Define filename and title
    if days == 1:
        filename = f"{timestamp_lp.strftime('%Y-%m-%dT%H.%M')}_binance_{fiat}-USDT_24h_price_data.png"
        save_path = config.TWENTY_FOUR_HOURS_PRICE_DIR / filename
        fig_title = f"Precio {fiat}/USDT en las Últimas 24 Horas"
    elif days == 7:
        filename = f"{timestamp_lp.strftime('%Y-%m-%dT%H.%M')}_binance_{fiat}-USDT_7d_price_data.png"
        save_path = config.ONE_WEEK_PRICE_DIR / filename
        fig_title = f"Precio {fiat}/USDT en los Últimos {days} Días"
    else:
        days = 14
        filename = f"{timestamp_lp.strftime('%Y-%m-%dT%H.%M')}_binance_{fiat}-USDT_14d_price_data.png"
        save_path = config.TWO_WEEKS_PRICE_DIR / filename
        fig_title = f"Precio {fiat}/USDT en los Últimos {days} Días"

    # Check if the graph already exists
    if save_path.exists():
        return save_path

    # Query data
    if days != 1:
        start_timestamp = (timestamp_lp - timedelta(days=days)).replace(hour=6, minute=0, second=0).astimezone(pytz.utc)
    else:
        start_timestamp = (timestamp_lp - timedelta(days=1)).replace(minute=0).astimezone(pytz.utc)
    df = mongo_controller.query_data(
        _mode="all",
        collection=f"USDT_{fiat}_Binance",
        _filter={"timestamp": {"$gte": start_timestamp}}
    )

    # Localize and convert timestamps
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = df['timestamp'].dt.tz_localize(pytz.utc).dt.tz_convert(la_paz_timezone)
    df.set_index('timestamp', inplace=True)

    # Compute cumulative trading time in hours for each timestamp
    trading_start = time(7, 0)
    trading_end = time(23, 59)
    active_seconds = (datetime.combine(datetime.today(), trading_end) -
                      datetime.combine(datetime.today(), trading_start)).seconds
    base_date = df.index[0].date()

    def compute_x(ts):
        day_offset = (ts.date() - base_date).days
        seconds_since_start = (ts - datetime.combine(ts.date(), trading_start, tzinfo=ts.tzinfo)).total_seconds()
        return day_offset * active_seconds + max(0, seconds_since_start)

    x_vals = df.index.map(compute_x) / 3600  # convert to hours

    # Plot setup
    fig, ax1 = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor('#001219')
    ax1.set_facecolor('#001219')

    # Plot volume
    vol_line, = ax1.plot(x_vals, df['sell_volume'], linestyle='dotted', alpha=0.5, label='Volumen (USDT)')
    ax1.set_xlabel('Fecha', color='white', fontweight='bold', fontsize=12, labelpad=10)
    ax1.set_ylabel('Volumen (USDT)', color='white', fontweight='bold', fontsize=12, labelpad=10)
    ax1.tick_params(axis='y', colors='white', labelsize=10)
    ax1.spines['left'].set_color('white')
    ax1.spines['bottom'].set_color('white')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)

    # Plot prices
    ax2 = ax1.twinx()
    buy_line, = ax2.plot(x_vals, df['sell_vwap'], label='Precio Venta USDT', color='#d62828')
    sell_line, = ax2.plot(x_vals, df['buy_vwap'], label='Precio Compra USDT', color='green')
    ax2.set_ylabel(f'Precio USDT ({fiat})', color='white', fontweight='bold', fontsize=12, labelpad=10)
    ax2.tick_params(axis='y', colors='white', labelsize=10)
    ax2.spines['right'].set_color('white')
    ax2.spines['bottom'].set_color('white')
    ax2.spines['top'].set_visible(False)
    ax2.spines['left'].set_visible(False)

    # Annotate current and start/end prices with % change
    start_price_sell = df['sell_vwap'].iloc[0]
    end_price_sell = df['sell_vwap'].iloc[-1]
    pct_change_sell = (end_price_sell - start_price_sell) / start_price_sell * 100
    start_price_buy = df['buy_vwap'].iloc[0]
    end_price_buy = df['buy_vwap'].iloc[-1]
    pct_change_buy = (end_price_buy - start_price_buy) / start_price_buy * 100
    annotation_text = (
        f"Venta: {end_price_sell:.2f} ({pct_change_sell:+.2f}%)\n"
        f"Compra: {end_price_buy:.2f} ({pct_change_buy:+.2f}%)"
    )
    # Place annotation at top-right inside plot
    ax2.text(
        0.5, 0.90, annotation_text,
        transform=ax2.transAxes,
        ha='center', va='top', fontsize=12,
        color='white', backgroundcolor='#001219', fontfamily='monospace',
    )

    # Y-axis limits and ticks
    ax2_tp, ax1_tp = calculate_ticker_positions_price_line_graph(
        df, ['buy_vwap', 'sell_vwap', 'sell_volume'], fiat)
    ax1.set_ylim(min(ax1_tp), max(ax1_tp))
    ax2.set_ylim(min(ax2_tp), max(ax2_tp))
    ax1.yaxis.set_major_locator(ticker.FixedLocator(ax1_tp))
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f"{int(x):,}"))
    ax2.yaxis.set_major_locator(ticker.FixedLocator(ax2_tp))

    # X-axis ticks
    if days == 1:
        # Determine start and end datetimes clamped to trading window
        start_dt = timestamp_lp - timedelta(days=1)
        start_dt = start_dt.replace(hour=max(trading_start.hour, min(start_dt.hour, trading_end.hour)), minute=0)
        end_dt = timestamp_lp.replace(hour=max(trading_start.hour, min(timestamp_lp.hour, trading_end.hour)), minute=0)
        hours_first = list(range(start_dt.hour, trading_end.hour + 1))
        hours_second = list(range(trading_start.hour, end_dt.hour + 1))
        tick_datetimes = [
                             datetime.combine(start_dt.date(), time(h, 0)) for h in hours_first
                         ] + [
                             datetime.combine(end_dt.date(), time(h, 0)) for h in hours_second
                         ]
        tick_hours = [(compute_x(dt) / 3600) for dt in tick_datetimes]
        tick_labels = [dt.strftime('%d/%m\n%H:%M') for dt in tick_datetimes]
    else:
        tick_days = range(days + 1)
        tick_hours = [(i * active_seconds) / 3600 for i in tick_days]
        tick_labels = [
            datetime.combine(base_date + timedelta(days=i), trading_start).strftime('%d/%m\n %H:%M')
            for i in tick_days
        ]
    ax1.set_xticks(tick_hours)
    ax1.set_xticklabels(tick_labels, fontsize=10)
    ax1.tick_params(axis='x', colors='white', which='both', labelsize=10, length=6)

    # Lower the plot lines
    vol_line.set_zorder(1)
    buy_line.set_zorder(1)
    sell_line.set_zorder(1)

    # Raise the spines above the lines
    for spine in ax1.spines.values():
        spine.set_zorder(3)
    for spine in ax2.spines.values():
        spine.set_zorder(3)

    # Ensure plot limits and layout
    ax1.set_xlim(tick_hours[0], x_vals.max())
    ax1.margins(x=0)
    ax2.margins(y=0)
    ax1.grid(which='both', color='white', linestyle='--', linewidth=0.5, alpha=0.3)
    fig.suptitle(fig_title, color='white', fontweight='bold', fontsize=16, y=0.92)
    lines = [vol_line, buy_line, sell_line]
    labels = [l.get_label() for l in lines]
    legend = fig.legend(lines, labels, loc='upper center', ncol=3, bbox_to_anchor=(0.5, 0.88),
                        facecolor='white', edgecolor='#001219', framealpha=1,
                        bbox_transform=fig.transFigure, fontsize=12, labelcolor='black', borderpad=0.5)
    legend.get_frame().set_edgecolor('black')
    plt.tight_layout(rect=[0.05, 0.05, 0.95, 0.95])

    # Save and return
    plt.savefig(save_path, dpi=300)
    plt.close(fig)
    return save_path


def liquidity_depth_chart(timestamp, fiat):
    """
    Generate a liquidity depth chart for a given cryptocurrency and fiat currency pair.

    Args:
        timestamp (datetime.now()): The timestamp for the data query.
        fiat (str): The fiat currency to query.

    Returns:
        int: Returns 0 upon successful execution.
    """
    # Manage timezones for saving the graph
    timestamp_utc = timestamp.replace(tzinfo=pytz.utc)
    timestamp_lp = timestamp_utc.astimezone(la_paz_timezone)
    filename = (
        f"{timestamp_lp.strftime('%Y-%m-%dT%H.%M')}_binance_{fiat}-USDT_liquidity_depth_graph.png"
    )
    save_path = config.LIQUIDITY_DEPTH_DIR / filename

    # Check if the graph already exists
    if save_path.exists():
        return save_path

    # Query the buy and sell liquidity depth data from the price record
    buy_liquidity_depth_df = mongo_controller.query_data(_mode="one", collection=f"USDT_{fiat}_Binance",
                                                         _filter={"timestamp": timestamp})["buy_liquidity_depth"]
    sell_liquidity_depth_df = mongo_controller.query_data(_mode="one", collection=f"USDT_{fiat}_Binance",
                                                          _filter={"timestamp": timestamp})["sell_liquidity_depth"]

    buy_liquidity_depth_df = pd.DataFrame(buy_liquidity_depth_df)
    sell_liquidity_depth_df = pd.DataFrame(sell_liquidity_depth_df)

    # Retrieve the buy VWAP and calculate the upper and lower bounds for filtering
    sell_vwap = mongo_controller.query_data(_mode="one", collection=f"USDT_{fiat}_Binance",
                                            _filter={"timestamp": timestamp})["sell_vwap"]
    sell_upper_bound = sell_vwap + (sell_vwap * 0.1)
    buy_lower_bound = sell_vwap - (sell_vwap * 0.1)

    # Filter the liquidity depth data based on the calculated bounds
    buy_liquidity_depth_df = buy_liquidity_depth_df[buy_liquidity_depth_df["price"] > buy_lower_bound]
    sell_liquidity_depth_df = sell_liquidity_depth_df[sell_liquidity_depth_df["price"] < sell_upper_bound]

    # Sort the DataFrame by price in ascending order for "buy" and descending for "sell"
    sell_liquidity_depth_df = sell_liquidity_depth_df.sort_values(by="price", ascending=True)
    buy_liquidity_depth_df = buy_liquidity_depth_df.sort_values(by="price", ascending=False)

    # Calculate the cumulative volume for buy and sell liquidity depth
    sell_liquidity_depth_df["cumulative_volume"] = sell_liquidity_depth_df["volume"].cumsum()
    buy_liquidity_depth_df["cumulative_volume"] = buy_liquidity_depth_df["volume"].cumsum()

    # Create a figure and axis for the plot
    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor('#001219')  # Set background color
    ax.set_facecolor('#001219')

    # Set the color of the axes labels, title, and legend to white
    ax.set_xlabel(f"Precio USDT ({fiat})", color='white', fontweight='bold', fontsize=12, labelpad=10)
    ax.set_ylabel("Volumen Acumulado (USDT)", color='white', fontweight='bold', fontsize=12, labelpad=10)
    ax.set_title(f"Gráfico de Profundidad de Liquidez {fiat}/USDT", color='white', fontweight='bold', fontsize=16)

    # Set the color of the tick labels to white
    ax.tick_params(axis='x', colors='white', labelsize=10)
    ax.tick_params(axis='y', colors='white', labelsize=10)

    # Set the color of the spines to white
    ax.spines['bottom'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.spines['top'].set_visible(False)
    ax.spines['left'].set_color('white')

    # Have y-axis ticks on both sides
    ax.yaxis.tick_right()
    ax.yaxis.set_ticks_position('both')
    ax.yaxis.set_label_position('left')
    ax.yaxis.set_tick_params(labelleft=True, labelright=True, labelsize=10)

    # Set the color of the grid to white
    ax.grid(which='both', color='white', linestyle='--', linewidth=0.5, alpha=0.3)

    # Adjust the plot to start at the axes
    ax.margins(x=0, y=0)

    # Set the major and minor ticks on the x and y axes
    if fiat == "BOB":
        ax.xaxis.set_major_locator(ticker.MultipleLocator(0.1))
        ax.xaxis.set_minor_locator(ticker.MultipleLocator(0.05))
        ax.yaxis.set_major_locator(ticker.MultipleLocator(50000))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(25000))
    elif fiat == "ARS":
        ax.xaxis.set_major_locator(ticker.MultipleLocator(10))
        ax.xaxis.set_minor_locator(ticker.MultipleLocator(5))
        ax.yaxis.set_major_locator(ticker.MultipleLocator(50000))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(25000))

    # Set the major and minor ticks length
    ax.tick_params(axis='x', which='major', length=6, colors='white', labelsize=8)
    ax.tick_params(axis='y', which='major', length=6, colors='white', labelsize=8)
    ax.tick_params(axis='x', which='minor', length=4, colors='white', labelsize=6)
    ax.tick_params(axis='y', which='minor', length=4, colors='white', labelsize=6)

    # Format the y-axis tick labels to include commas
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{int(x):,}'))

    # Plot sell liquidity depth as an area chart
    sns.lineplot(data=buy_liquidity_depth_df, x="price", y="cumulative_volume", ax=ax, label=f"Demanda USDT",
                 color="green")  # Green
    ax.fill_between(buy_liquidity_depth_df["price"], buy_liquidity_depth_df["cumulative_volume"], alpha=0.3,
                    color="green")  # Green

    # Plot buy liquidity depth as an area chart
    sns.lineplot(data=sell_liquidity_depth_df, x="price", y="cumulative_volume", ax=ax, label=f"Oferta USDT",
                 color="#d62828")  # Red
    ax.fill_between(sell_liquidity_depth_df["price"], sell_liquidity_depth_df["cumulative_volume"], alpha=0.3,
                    color="#d62828")  # Red

    ax.tick_params(labelsize=10)

    # Add legend to the plot
    ax.legend(facecolor='white', edgecolor='white', framealpha=1, fontsize=12)

    plt.savefig(save_path, dpi=300)
    plt.close(fig)
    return save_path


def calculate_ticker_positions_price_line_graph(dataframe, columns, fiat):
    """
    Calculate the ticker positions for the price line graph.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing the data.
        columns (list): A list of column names to be used for calculations.
                        The first two columns are used for buy/sell prices,
                        and the third column is used for volume.
        fiat (str): The fiat currency to determine the step size and margin.

    Returns:
        tuple: A tuple containing two lists:
               - buy_sell_ticker_positions (list): Ticker positions for the buy/sell axis.
               - volume_ticker_positions (list): Ticker positions for the volume axis.
    """
    # TODO: Examples are outdated
    increase_step = 0
    while True:
        if fiat == "BOB":
            possible_step_sizes = [0.05, 0.1, 0.2, 0.25, 0.5, 1]
            price_step_size = possible_step_sizes[increase_step]
            price_margin_size = price_step_size * 2
            # Calculate the number of steps for the graph, for the buy/sell axis, with price_step_size
            sell_max = int(
                (dataframe[columns[1]].max() + price_margin_size) * 100)  # [BOB]Example: 10.32 -> 10.47 -> 1047
            buy_min = dataframe[columns[0]].min() - price_margin_size  # [BOB]Example: 10.22 -> 10.07
            if increase_step <= 2:
                buy_min = float(str(round(buy_min, 1)))  # [BOB]Example: 10.07 -> 10.1
            else:
                buy_min = float(str(round(buy_min, 0)))  # [BOB]Example: 10.07 -> 10
            if buy_min % price_step_size != 0:
                buy_min = buy_min - (buy_min % price_step_size)
            buy_min = int(buy_min * 100)  # [BOB]Example: 10.1 -> 1010
            price_step_size = int(price_step_size * 100)  # [BOB]Example: 0.05 -> 5
        else:  # ARS
            possible_step_sizes = [5, 10, 20, 25, 50, 100]
            price_step_size = possible_step_sizes[increase_step]
            price_margin_size = price_step_size * 3
            # Calculate the number of steps for the graph, for the buy/sell axis, with price_step_size
            sell_max = int(dataframe[columns[0]].max() + price_margin_size)  # [ARS]Example: 1205.53 -> 1220.53 -> 1220
            buy_min = int(dataframe[columns[1]].min() - price_margin_size)  # [ARS]Example: 1194.45 -> 1179.45 -> 1179
            buy_min = float(str(round(buy_min / 10, 0)))  # Example: [ARS]1179 -> 117.9 -> 118
            buy_min = int(buy_min * 10)  # [ARS]Example: 118 -> 1180

        # Calculate the ticker positions based on the buy/sell axis
        buy_sell_ticker_positions = list(range(buy_min, sell_max + price_step_size, price_step_size))
        # [BOB]Example: [1010, 1015, 1020, ..., 1050]
        if fiat == "BOB":
            # Transform the ticker positions back to numbers with two decimals
            for i in range(len(buy_sell_ticker_positions)):
                buy_sell_ticker_positions[i] = round(buy_sell_ticker_positions[i] / 100,
                                                     2)  # [BOB]Example: 1010 -> 10.1
        # Compute the number of steps for the graph, based on the ticker positions from the buy/sell axis
        steps = len(buy_sell_ticker_positions)  # [BOB]Example: 9
        if steps > 15:
            increase_step += 1
        else:
            break

    # Now calculate the positions for the volume axis, based on the number of steps for the buy/sell axis
    possible_volume_step_sizes = [50000, 100000, 200000, 500000, 1000000, 2000000, 5000000]
    increase_step = 0
    volume_max = dataframe[columns[2]].max()
    while True:
        volume_step_size = possible_volume_step_sizes[increase_step]
        volume_ticker_positions = [0]
        for i in range(steps - 1):
            volume_ticker_positions.append(volume_ticker_positions[i] + volume_step_size)
        if volume_ticker_positions[-1] < volume_max:
            increase_step += 1
        else:
            break

    return buy_sell_ticker_positions, volume_ticker_positions


if __name__ == "__main__":
    # Get the latest timestamp from the database
    latest_record = mongo_controller.query_data(_mode="all", collection="USDT_BOB_Binance",
                                                _filter={}, sort=-1, limit=1)
    # Get the latest timestamp
    latest_timestamp = latest_record["timestamp"].iloc[0].to_pydatetime()
    print("[graph_generator] Generating 1-day line graph...")
    line_graph_x_days_price(latest_timestamp, fiat="BOB", days=1)
    # print("[graph_generator] Generating 7-day line graph...")
    # line_graph_x_days_price(latest_timestamp, fiat="BOB", days=7)
    # print("[graph_generator] Generating 14-day line graph...")
    # line_graph_x_days_price(latest_timestamp, fiat="BOB", days=14)
    # print("[graph_generator] Generating all-time line graph...")
    # line_graph_x_days_price(latest_timestamp, fiat="BOB", days=None)

    # Generate a liquidity depth chart for a given cryptocurrency and fiat currency pair
    print("[graph_generator] Generating liquidity depth chart for BOB...")
    liquidity_depth_chart(latest_timestamp, fiat="BOB")
    # liquidity_depth_chart(latest_timestamp, fiat="ARS")
