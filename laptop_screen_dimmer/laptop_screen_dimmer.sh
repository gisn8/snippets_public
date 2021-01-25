#!/bin/bash
# Runs when power source switched form AC to battery.
# True when on battery.
# Sources:
# https://gist.github.com/andreafortuna/6eea255e1846c894d46c4b7d8b813878
# https://stackoverflow.com/questions/10415064/how-to-calculate-the-minimum-of-two-variables-simply-in-bash

basedir="/sys/class/backlight/"

# get the backlight handler
handler=$basedir$(ls $basedir)"/"

# current brightness
old_brightness=$(cat $handler"brightness")

# max brightness
max_brightness=$(cat $handler"max_brightness")

# current %
old_brightness_p=$(( 100 * $old_brightness / $max_brightness ))

# new %
if [ "$1" == "true" ]; then
    # dim screen by 35%
    new_brightness_p=$(($old_brightness_p -35))
else
    # brighten screen by %35
    new_brightness_p=$(($old_brightness_p +35))
fi

# new brightness value
new_brightness=$(( $max_brightness * $new_brightness_p / 100 ))

# set the new brightness value
sudo chmod 666 $handler"brightness"

# To get MIN(max_brightness,new_brightness) USE $((max_brightness < new_brightness ? max_brightness : new_brightness))
# sudo echo $new_brightness > $handler"brightness"
sudo echo $((max_brightness < new_brightness ? max_brightness : new_brightness)) > $handler"brightness"
