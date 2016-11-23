import argparse

def main(x_center, y_center, scale, max_iteration = 1000, x_size = 100, y_size = 50):
    lines = []
    for j in range(y_size):
        line = []
        for i in range(x_size):
            x,y = ( x_center + scale*float(i-x_size/2)/x_size,
                      y_center + scale*float(j-y_size/2)/y_size
                    )

            a,b = (0.0, 0.0)
            iteration = 0

            while (a**2 + b**2 <= 4.0 and iteration < max_iteration):
                a,b = a**2 - b**2 + x, 2*a*b + y
                iteration += 1

            line.append(iteration)
        lines.append(line)

    for line in lines:
        print("".join([" " if x == max_iteration else "#" for x in line]))

if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument("x_center", type=float)
    parse.add_argument("y_center", type=float)
    parse.add_argument("zoom", type=float)

    args = parse.parse_args()
    main(args.x_center, args.y_center, 1.0/args.zoom)

