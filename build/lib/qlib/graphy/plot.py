try:
    import matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    matplotlib.style.use('ggplot')
    import numpy as np
    from matplotlib.lines import Line2D
except ImportError as e:
    LogControl.err("pip3 install matploit numpy")
from qlib.log import LogControl
LogControl.LOG_LEVEL = LogControl.INFO


class Drawer:
    """
    for drawer something 
    this is a power and abstract,
    put container in this , then ,just draw it or animation it
    """
    def __init__(self, container, x=[0,100], y=[0,256] ,ion=False, **options):
        if ion:
            plt.ion()
        
        self.fig, self.ax = plt.subplots()
        self.x = x
        self.y = y
        self.ax.set_xlim(x)
        self.ax.set_ylim(y)
        self.container = container(self.ax, **options)
        self.ani = None
        self.data_gen = None

    def update(self, *args, **kargs):
        return self.container.update(*args, **kargs)

    def show(self):
        plt.show()

    def draw(self):
        self.container.draw()

    def set_data(self, data):
        self.container.set_data(data)

    def set_data_gen(self, data_gen_func):
        self.data_gen = data_gen_func()

    def animal(self, interval=24):
        self.ani = animation.FuncAnimation(
            self.fig, 
            self.update,  #this function to update each step
            self.data_gen,  # this function will generator a data
            interval=interval, # set intervial time x frame each second
            blit=False)
        plt.show()


class Line:
    """
    ax: fig's axis
    dt: distance each step, default 1
    """
    def __init__(self, ax, dt=0.4, line_num=1,**options):
        self.ax = ax
        self.dt = dt
        for i in range(line_num):
            line = Line2D([0], [0])
            self.ax.add_line(line)
        if "style" in options:
            self.style = options['style']

    def update(self, ys):
        for i, y in enumerate(ys):
            try:
                line = self.ax.get_lines()[i]
                tdata = line.get_xdata()
                ydata = line.get_ydata()
                
                
                t = tdata[-1] + self.dt # new x data
                tdata.append(t)
                ydata.append(y)

                self.resize_panel(t, y) # resize the fig 

                line.set_data(tdata, ydata)
            except IndexError:
                line = Line2D([0], [0])
                self.ax.add_line(line)
            except Exception as e:
                LogControl.err(e)

        return self.ax.get_lines()

    def resize_panel(self, t, y):
        lastt = t
        lasty = y
        change = False
        xlim_l, xlim_r = self.ax.get_xlim()
        ylim_l, ylim_r = self.ax.get_ylim()
        print(lastt, lasty)
        if lastt > xlim_r:  # reset the arrays

            change = True
            xlim_r *= 1.3
            self.ax.set_xlim(xlim_l, xlim_r)
            

        if lasty > ylim_r:
            change = True
            ylim_r *= 1.3
            self.ax.set_ylim(ylim_l, ylim_r)
        if lasty < ylim_l:
            change = True
            ylim_l -= ylim_r/3
            self.ax.set_ylim(ylim_l, ylim_r)


        if change:
            self.ax.figure.canvas.draw()
            change = False

    # def lim_change(self, style='append'):
    #     if style

    def draw(self):
        pass


class Kde:

    def __init__(self, ax):
        self.ax = ax

    def set_data(self, pd_data):
        self.data = pd_data

    def draw(self):
        self.data.plot(kind='kde')


# x = np.arange(0, 2*np.pi, 0.01)
# line, = ax.plot(x, np.sin(x))


# def animate(i):
#     line.set_ydata(np.sin(x + i/10.0))  # update the data
#     return line,


# # Init only required for blitting to give a clean slate.
# def init():
#     line.set_ydata(np.ma.array(x, mask=True))
#     return line,

# ani = animation.FuncAnimation(fig, animate, np.arange(1, 200), init_func=init,
#                               interval=25, blit=True)
# plt.show()
