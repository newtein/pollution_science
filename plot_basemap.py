import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
#import seaborn as sns
import xlsxwriter
import scipy.stats as stats
import os,sys
import pickle
import random
import math
import time
import numpy as np
import networkx as nx
import pandas as pd
from mpl_toolkits.basemap import Basemap
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon, Rectangle
from collections import OrderedDict

# lower_left_lon=-129.75
# lower_left_lat= 20.91
# upper_right_lon=-64.63
# upper_right_lat= 50.08
#california
lower_left_lon=-125
lower_left_lat= 32
upper_right_lon=-113
upper_right_lat= 43

class PlotBaseMap:
    def __init__(self, filename, pollutant, label='a)'):
        self.filename = filename
        self.pollutant = pollutant
        self.label =label
        self.df = pd.read_csv(self.filename)
        self.skip = []
        self.site_1 = set()
        self.nodeNames = self.get_node_names()
        print("Node names created")
        self.Bmap_Amplitude, self.G = self.make_network()
        print("Network created")


    def get_node_names(self):
        nodesnames = {}
        for index, row in self.df.iterrows():
            n1, n2 = row['site_id_1'], row['site_id_2']
            self.site_1.add(n1)
            location = eval(row['haversine'])
            l1, l2 = location[0], location[1]
            if n1 not in nodesnames:
                nodesnames[n1] = l1
            if n2 not in nodesnames:
                nodesnames[n2] = l2
        return nodesnames

    def validateLocation(self, longitude, latitude):
        if (lower_left_lon < longitude < upper_right_lon) and (lower_left_lat < latitude < upper_right_lat):
            return True
        return False

    def make_network(self):
        resolution = 'l'
        Bmap_Amplitude = Basemap(projection='merc', llcrnrlon=lower_left_lon, llcrnrlat=lower_left_lat,
                                 urcrnrlon=upper_right_lon, urcrnrlat=upper_right_lat, lat_ts=0, resolution=resolution,
                                 suppress_ticks=True)
        G_Amplitude = nx.Graph()
        for name in self.nodeNames:
            latitude = self.nodeNames[name][1]
            longitude = self.nodeNames[name][0]
            if self.validateLocation(latitude, longitude):
                x, y = Bmap_Amplitude(latitude, longitude)
                G_Amplitude.add_node(name, pos=(x, y))
            else:
                self.skip.append(name)
        for index, row in self.df.iterrows():
            n1, n2 = row['site_id_1'], row['site_id_2']
            weight = row['correlation']
            if n1 not in self.skip and n2 not in self.skip:
                G_Amplitude.add_edge(n1, n2, weight=weight)

        return Bmap_Amplitude, G_Amplitude

    def plot_network(self):
        for _id in self.skip:
            try:
                del self.nodeNames[_id]
            except:
                pass
        cmap = plt.get_cmap("GnBu")
        nodeList = list(self.nodeNames)
        edgewidth_Amplitude = [d['weight'] for (u, v, d) in self.G.edges(data=True)]
        pos = nx.get_node_attributes(self.G, 'pos')
        print(self.site_1)
        nodeColor = ['r' if i in self.site_1 else 'grey' for i in nodeList]
        nodeSize = [7 if i in self.site_1 else 2 for i in nodeList]
        plt.close()
        plt.axis('off')
        nx.draw_networkx_nodes(self.G, pos, node_color=nodeColor, nodelist=nodeList, node_size=nodeSize, alpha=0.85)
        edgeColor = []
        for node1, node2, d in self.G.edges(data=True):
            colorMark = cmap(d['weight'])
            edgeColor.append(colorMark)
        nx.draw_networkx_edges(self.G, pos, cmap=cmap, edge_color =  edgeColor,
                               width=edgewidth_Amplitude)

        shp_info = self.Bmap_Amplitude.readshapefile('st99_d00', 'states', drawbounds=True)

        ax = plt.gca()  # get current axes instance
        ax.text(-0.05, 0.98, self.label, transform=ax.transAxes, size=15, color='green')

        for _, seg in enumerate(self.Bmap_Amplitude.states):
            # skip DC and Puerto Rico.
            poly = Polygon(seg, facecolor='white', edgecolor='green', alpha=0.9, zorder=-0.1, linewidth=0.05)
            ax.add_patch(poly)

        plt.axis('off')

        plt.title(self.pollutant)
        # plt.text(40, 37, "Degree Distribution", fontsize=17, ha="center")
        plt.savefig("basemap/{}.png".format(self.pollutant), dpi=600, bbox_inches='tight')

if __name__ == "__main__":
    """
    path = 
    """
    pollutants = ["PM2", "PM10", "O3"]
    label = ["a)", "b)", "c)"]
    for index, pollutant in enumerate(pollutants):
        filename ="files/{}/{}_24-03-2020_07-04-2020.csv".format(pollutant, pollutant)
        PlotBaseMap(filename, pollutant, label=label[index]).plot_network()

