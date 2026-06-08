// Flight preview - nose forward
module fuselage() {
    difference() {
        union() {
            translate([0, 0, 140])
                rotate_extrude(angle = 360)
                    intersection() {
                        hull() {
                            translate([21.5, 0]) circle(d = 3);
                            translate([1, 55]) circle(d = 2);
                        }
                        square([23, 57]);
                    }
            cylinder(d = 46, h = 140);
            translate([0, 0, -25])
                cylinder(d1 = 40, d2 = 46, h = 25);
        }
        translate([0, 0, -26])
            cylinder(d = 36, h = 200);
        for (rot = [0, 90])
            rotate([0, 0, rot])
                translate([-24, -9.5, 61.5])
                    cube([48, 19, 6]);
        for (z = [57, 69])
            for (rot = [0, 90])
                rotate([0, 0, rot])
                    translate([0, 0, z])
                        rotate([90, 0, 0])
                            cylinder(d = 3.2, h = 56, center = true);
        for (rot = [45, 135, 225, 315])
            rotate([0, 0, rot])
                translate([22, 0, 108])
                    rotate([90, 0, 0])
                        cylinder(d = 4, h = 8);
        translate([0, 21, 135])
            rotate([90, 0, 0])
                cylinder(d = 14, h = 6, center = true);
        for (x = [-1, 1])
            translate([x * 8, 21, 135])
                rotate([90, 0, 0])
                    cylinder(d = 2.2, h = 6, center = true);
        for (rot = [0, 180])
            rotate([0, 0, rot])
                translate([0, 22, 5])
                    cube([4, 4, 20], center = true);
        translate([0, 0, 125])
            cylinder(d = 36, h = 12);
        translate([0, 0, 10])
            cylinder(d = 36, h = 15);
    }
}

module arm() {
    clamp_len = 22;
    arm_len = 87;
    total_arm = clamp_len + arm_len;
    
    difference() {
        hull() {
            translate([0, 0, 0])
                linear_extrude(height = 5)
                    square([18, clamp_len], center = true);
            translate([0, total_arm, 0])
                linear_extrude(height = 5)
                    square([18, 0.1], center = true);
        }
        translate([0, total_arm - 8, 5])
            hull() {
                translate([-17, 0, 0])
                    cube([34, 14, 3]);
                translate([-7, -2, 0])
                    cube([14, 4, 0.1]);
            }
        for (z = [clamp_len/2 - 5, clamp_len/2 + 5])
            translate([0, z, 2.5])
                rotate([90, 0, 0])
                    cylinder(d = 3.2, h = 38, center = true);
        translate([0, total_arm, -1]) {
            for (x = [-1, 1], y = [-1, 1])
                translate([x * 8, y * 8])
                    cylinder(d = 3.2, h = 11);
            cylinder(d = 5, h = 11);
        }
        translate([0, clamp_len + 10, 2.5])
            hull() {
                circle(d = 4);
                translate([0, arm_len - 16, 0])
                    circle(d = 4);
            }
    }
}

$fn = 48;

// Assembly in flight orientation (nose forward = X+)
color("#5577aa", 0.9) rotate([0, 90, 0]) fuselage();

color("#dd7733", 0.9)
    for (i = [0:3]) {
        angle = i * 90 + 45;
        rotate([0, 90, 0]) rotate([0, 0, angle])
            translate([22, 0, 62.5])
                arm();
    }
