// ============================================================
// INTERCEPTOR DRONE FRAME - v2
// Bullet-style fuselage, X-config arms, tailsitter layout
// 5" props, 2207 motors, tube battery
// ============================================================

// --- USER PARAMETERS ---
wheelbase      = 220;      // mm diagonal motor-to-motor
body_diam      = 44;       // mm max fuselage diameter
battery_id     = 36;       // mm inner bore for battery
body_len       = 160;      // mm fuselage length (main body)
nose_len       = 50;       // mm nose cone length
tail_len       = 30;       // mm tail section length
arm_width      = 14;       // mm arm width (flat section)
arm_thick      = 5;        // mm arm thickness
motor_screw    = 16;       // mm motor mount screw pattern
motor_od       = 28;       // mm typical 2207 motor outer diameter
stack_w        = 30;       // mm 30x30 stack

$fn            = 48;

// --- DERIVED ---
arm_span    = wheelbase / 2;                       // center to motor
body_radius  = body_diam / 2;
nose_radius  = body_radius - 2;
total_len    = body_len + nose_len + tail_len;

echo(str("Wheelbase: ", wheelbase, " mm"));
echo(str("Arm length: ", arm_span - body_radius, " mm"));
echo(str("Total length: ", total_len, " mm"));

// ============================================================
// MODULE: Fuselage Body (missile/bullet shape)
// ============================================================
module fuselage() {
    difference() {
        union() {
            // Nose cone (ogive profile)
            rotate_extrude(angle = 360)
                translate([0, 0])
                    hull() {
                        translate([nose_radius - 0.5, nose_len])
                            circle(d = 2);
                        translate([body_radius - 2, 0])
                            circle(d = 3);
                    }
            
            // Main cylindrical body
            translate([0, 0, nose_len])
                cylinder(d = body_diam, h = body_len);
            
            // Tail taper
            translate([0, 0, nose_len + body_len])
                cylinder(d1 = body_diam, d2 = body_diam - 6, h = tail_len);
        }
        
        // Battery bore (through the whole body)
        translate([0, 0, -1])
            cylinder(d = battery_id, h = total_len + 2);
        
        // Arm cross-slots (X configuration, centered in body)
        slot_z = nose_len + body_len / 2;
        for (rot = [0, 90])
            rotate([0, 0, rot])
                translate([-(body_diam+4)/2, -(arm_width+1)/2, slot_z - arm_thick/2 - 0.25])
                    cube([body_diam+4, arm_width+1, arm_thick + 0.5]);
        
        // Arm clamp bolt holes (cross-body)
        for (z = [slot_z - 8, slot_z + 8])
            for (rot = [0, 90])
                rotate([0, 0, rot])
                    translate([0, 0, z])
                        rotate([90, 0, 0])
                            cylinder(d = 3.2, h = body_diam + 10, center = true);
        
        // Cooling vents (between arms)
        for (rot = [45, 135, 225, 315])
            rotate([0, 0, rot])
                for (z = [nose_len + 10, nose_len + body_len - 10])
                    translate([body_radius - 1, 0, z])
                        rotate([90, 0, 0])
                            cylinder(d = 3, h = 6);
        
        // Camera hole (front side)
        translate([0, body_radius - 3, nose_len + 5])
            rotate([90, 0, 0])
                cylinder(d = 10, h = 8, center = true);
        
        // Camera mounting holes (M2)
        for (x = [-1, 1])
            translate([x * 6, body_radius - 3, nose_len + 5])
                rotate([90, 0, 0])
                    cylinder(d = 2.2, h = 8, center = true);
        
        // Electronics access hole (top of body)
        translate([0, 0, nose_len + body_len / 2])
            cylinder(d = 12, h = body_len / 2 + 2);
    }
}

// ============================================================
// MODULE: Arms (4 individual arms with motor mounts)
// Print 4 copies. Arm is a wing-like tapered shape.
// ============================================================
module arm() {
    arm_len = arm_span - body_radius;
    clamp_len = 20;
    
    difference() {
        union() {
            // Arm body - tapered like a wing
            linear_extrude(height = arm_thick)
                hull() {
                    // Clamp section (wider where it meets body)
                    translate([0, 0])
                        square([arm_width + 8, clamp_len]);
                    // Motor mount section (wider at tip)
                    translate([-(arm_width + 6)/2, clamp_len + arm_len])
                        square([arm_width + 6, 0.1]);
                }
            
            // Motor mount pad (thicker at end)
            translate([0, clamp_len + arm_len - 6, 0])
                hull() {
                    translate([-(motor_od + 6)/2, 0, 0])
                        cube([motor_od + 6, 12, arm_thick + 4]);
                    translate([-(arm_width + 4)/2, -2, arm_thick])
                        cube([arm_width + 4, 4, 0.1]);
                }
        }
        
        // Clamp bolt holes
        for (z = [clamp_len/2 - 4, clamp_len/2 + 4])
            translate([0, z, arm_thick/2])
                rotate([90, 0, 0])
                    cylinder(d = 3.2, h = arm_width + 20, center = true);
        
        // Motor mount screw holes (16mm and 19mm patterns)
        translate([0, clamp_len + arm_len, -1]) {
            for (a = [motor_screw])
                for (x = [-1, 1], y = [-1, 1])
                    translate([x * a/2, y * a/2])
                        cylinder(d = 3.2, h = arm_thick + 6);
            // Center hole
            cylinder(d = 5, h = arm_thick + 6);
        }
        
        // Weight reduction
        translate([0, clamp_len + 10, arm_thick/2 - 1])
            hull() {
                circle(d = 4);
                translate([0, arm_len - 14, 0])
                    circle(d = 4);
            }
    }
}

// ============================================================
// MODULE: Tail Fins / Landing Gear (4 fins)
// ============================================================
module tail_fin() {
    fin_height = tail_len + 5;
    fin_chord  = 30;    // mm root chord
    fin_tip    = 12;    // mm tip chord
    fin_thick  = 3;     // mm
    
    // Triangular fin profile
    difference() {
        hull() {
            // Root (attached to body)
            translate([0, 0, 0])
                linear_extrude(height = fin_thick)
                    square([fin_chord, 0.1]);
            // Tip (outer)
            translate([5, fin_height, 0])
                linear_extrude(height = fin_thick)
                    square([fin_tip, 0.1]);
        }
        
        // Mounting hole for body
        translate([0, fin_height - 5, fin_thick/2])
            rotate([90, 0, 0])
                cylinder(d = 3.2, h = 8, center = true);
    }
}

// ============================================================
// MODULE: Stack Mount Plate (30x30 on top of body)
// ============================================================
module stack_plate() {
    difference() {
        union() {
            // Mounting plate with curvature for body
            translate([0, 0, 0])
                linear_extrude(height = 3)
                    hull() {
                        for (x = [-1, 1], y = [-1, 1])
                            translate([x * 15.5, y * 15.5])
                                circle(d = 6);
                    }
            // Standoff posts
            for (x = [-1, 1], y = [-1, 1])
                translate([x * 15.5, y * 15.5, -6])
                    cylinder(d = 5, h = 6);
        }
        
        // 30x30 mounting holes
        for (x = [-1, 1], y = [-1, 1])
            translate([x * 15.5, y * 15.5, -7])
                cylinder(d = 3.2, h = 12);
        
        // Concave bottom to fit body curvature
        translate([0, 0, -7])
            cube([body_diam, body_diam, 10]);
    }
}

// ============================================================
// MODULE: Boresight / Camera Mount
// ============================================================
module camera_mount() {
    difference() {
        union() {
            // Camera bracket
            cube([16, 10, 8]);
        }
        // Camera lens hole
        translate([8, -1, 4])
            rotate([90, 0, 0])
                cylinder(d = 8, h = 12);
        // Mounting holes
        for (x = [-1, 1])
            translate([x * 6 + 8, -1, 4])
                rotate([90, 0, 0])
                    cylinder(d = 2.2, h = 12);
    }
}

// ============================================================
// ASSEMBLY PREVIEW
// ============================================================
module assembly() {
    color("#6688aa") fuselage();
    color("#dd8844") 
        for (i = [0:3])
            rotate([0, 0, i * 90 + 45])
                translate([body_radius - 3, 0, nose_len + body_len/2 - arm_thick/2 - 0.25])
                    rotate([0, 0, -45 + i * 90 + 45])
                        arm();
    color("#44aa44")
        for (i = [0:3])
            rotate([0, 0, i * 90 + 45])
                translate([body_radius - 2, 0, nose_len + body_len + tail_len - tail_len/2])
                    rotate([0, 0, -45 + i * 90 + 45])
                        tail_fin();
    color("#ccaa22") translate([0, 0, nose_len + body_len - 15]) stack_plate();
}

// ============================================================
// RENDER SELECTION
// ============================================================
which_part = "assembly"; // "fuselage" "arm" "tail_fin" "stack_plate" "camera_mount" "assembly"

if (which_part == "fuselage")    fuselage();
if (which_part == "arm")         arm();
if (which_part == "tail_fin")    tail_fin();
if (which_part == "stack_plate") stack_plate();
if (which_part == "camera_mount") camera_mount();
if (which_part == "assembly")    assembly();
