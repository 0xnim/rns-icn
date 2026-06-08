// ============================================================
// INTERCEPTOR DRONE FRAME - v3
// Horizontal quad layout, rocket fuselage, X-config arms
// 5" props, 2207 motors, tube battery through body
// ============================================================

// --- PARAMETERS ---
// Frame
wheelbase      = 220;      // mm diagonal motor-to-motor
body_diam      = 46;       // mm fuselage diameter  
battery_id     = 36;       // mm battery bore inner diameter
body_len       = 140;      // mm central body length
nose_len       = 55;       // mm nose cone length
tail_len       = 25;       // mm tail taper length

// Arms
arm_root_w     = 18;       // mm arm width at body
arm_tip_w      = 12;       // mm arm width at motor
arm_thick      = 5;        // mm arm thickness
arm_profile_h  = 10;       // mm arm airfoil height at root

// Motors
motor_screw    = 16;       // mm M3 pattern (16x16 or 19x19)
motor_od       = 28;       // mm 2207 motor diameter

// Hardware
bolt_d         = 3.2;      // mm M3 hole
$fn            = 48;

// --- DERIVED ---
arm_span    = wheelbase / 2;   // center to motor
arm_len     = arm_span - body_diam/2;
total_len   = body_len + nose_len + tail_len;
nose_rad    = body_diam/2 - 1.5;

echo(str("Arm length: ", arm_len, " mm"));
echo(str("Total length: ", total_len, " mm"));

// ============================================================
// FUSELAGE - Bullet-shaped body (horizontal orientation)
// Nose at front (Z+), tail at rear (Z-)
// ============================================================
module fuselage() {
    difference() {
        union() {
            // Nose cone - ogive profile (at front/Top)
        // All 2D geometry must be on X >= 0 for rotate_extrude
        translate([0, 0, total_len - nose_len])
            rotate_extrude(angle = 360)
                intersection() {
                    hull() {
                        translate([nose_rad - 1, 0])
                            circle(d = 3);
                        translate([1, nose_len])
                            circle(d = 2);
                    }
                    square([body_diam/2, nose_len + 2]);
                }
            
            // Main body cylinder
            cylinder(d = body_diam, h = body_len);
            
            // Tail taper (at rear/Bottom)
            translate([0, 0, -tail_len])
                cylinder(d1 = body_diam - 6, d2 = body_diam, h = tail_len);
        }
        
        // Battery bore - through entire body
        translate([0, 0, -tail_len - 1])
            cylinder(d = battery_id, h = total_len + 2);
        
        // ============================================
        // ARM SLOTS - X configuration through body
        // Two perpendicular slots at arm attachment Z
        // ============================================
        slot_z = body_len * 0.45;  // arm position (slightly forward of center)
        slot_h = arm_thick + 0.5;
        slot_w = arm_root_w + 1;
        
        for (rot = [0, 90])
            rotate([0, 0, rot])
                translate([-(body_diam+2)/2, -slot_w/2, slot_z - slot_h/2])
                    cube([body_diam+2, slot_w, slot_h]);
        
        // Arm clamp bolt holes (M3 cross-bolts through body)
        for (z = [slot_z - 6, slot_z + 6])
            for (rot = [0, 90])
                rotate([0, 0, rot])
                    translate([0, 0, z])
                        rotate([90, 0, 0])
                            cylinder(d = bolt_d, h = body_diam + 10, center = true);
        
        // ============================================
        // COOLING VENTS
        // ============================================
        for (rot = [45, 135, 225, 315])
            rotate([0, 0, rot])
                translate([body_diam/2 - 1, 0, body_len * 0.75])
                    rotate([90, 0, 0])
                        cylinder(d = 4, h = 8);
        
        // ============================================
        // CAMERA CUTOUT (front-top of body)
        // ============================================
        translate([0, body_diam/2 - 2, body_len - 5])
            rotate([90, 0, 0])
                cylinder(d = 14, h = 6, center = true);
        
        // Camera mount holes (M2)
        for (x = [-1, 1])
            translate([x * 8, body_diam/2 - 2, body_len - 5])
                rotate([90, 0, 0])
                    cylinder(d = 2.2, h = 6, center = true);
        
        // ============================================
        // BATTERY STRAP SLOTS (rear)
        // ============================================
        for (rot = [0, 180])
            rotate([0, 0, rot])
                translate([0, body_diam/2 - 1, 5])
                    cube([4, 4, 20], center = true);
        
        // ============================================
        // WEIGHT REDUCTION (internal pockets)
        // ============================================
        // Between arms and nose
        translate([0, 0, body_len - 15])
            cylinder(d = body_diam - 10, h = 12);
        // Between arms and tail
        translate([0, 0, 10])
            cylinder(d = body_diam - 10, h = 15);
    }
}

// ============================================================
// ARM - airfoil profile with motor mount
// Print 4 copies, flat on bed
// ============================================================
module arm() {
    clamp_len = 22;       // mm clamp section length
    total_arm = clamp_len + arm_len;
    
    // 2D airfoil profile for the arm
    module airfoil_section(w, h) {
        scale([1, h/w/2])
            circle(d = w);
    }
    
    difference() {
        union() {
            // Main arm body - tapered airfoil
            // Use hull of linear_extrude+scale... simpler: just tapered arm
            hull() {
                // Clamp root
                translate([0, 0, 0])
                    linear_extrude(height = arm_thick)
                        square([arm_root_w, clamp_len], center = true);
                
                // Motor mount (wider at tip)
                translate([0, total_arm, 0])
                    linear_extrude(height = arm_thick)
                        square([arm_tip_w + 6, 0.1], center = true);
            }
            
            // Motor mount pad (thickened at tip)
            translate([0, total_arm - 8, arm_thick])
                hull() {
                    translate([-(motor_od + 6)/2, 0, 0])
                        cube([motor_od + 6, 14, 3]);
                    translate([-(arm_tip_w + 2)/2, -2, 0])
                        cube([arm_tip_w + 2, 4, 0.1]);
                }
            
            // Gimbal/support webbing at root
            translate([0, 0, 0])
                hull() {
                    translate([-(body_diam/2), 0, 0])
                        cube([0.1, clamp_len, arm_thick + 2]);
                    translate([-(arm_root_w/2), 0, 0])
                        cube([arm_root_w, clamp_len, arm_thick]);
                }
        }
        
        // Clamp bolt holes (through flat of arm)
        for (z = [clamp_len/2 - 5, clamp_len/2 + 5])
            translate([0, z, arm_thick/2])
                rotate([90, 0, 0])
                    cylinder(d = bolt_d, h = arm_root_w + 20, center = true);
        
        // Motor mount holes
        translate([0, total_arm, -1]) {
            for (x = [-1, 1], y = [-1, 1])
                translate([x * motor_screw/2, y * motor_screw/2])
                    cylinder(d = bolt_d, h = arm_thick + 6);
            cylinder(d = 5, h = arm_thick + 6);
        }
        
        // Weight reduction pockets
        translate([0, clamp_len + 10, arm_thick/2]) {
            hull() {
                circle(d = 4);
                translate([0, arm_len - 16, 0])
                    circle(d = 4);
            }
        }
    }
}

// ============================================================
// STACK MOUNT - 30x30 FC/ESC on top of body
// ============================================================
module stack_mount() {
    difference() {
        union() {
            // Plate curved to fit body
            intersection() {
                translate([0, 0, -3])
                    cylinder(d = body_diam + 10, h = 3);
                hull() {
                    for (x = [-1, 1], y = [-1, 1])
                        translate([x * 15.5, y * 15.5])
                            circle(d = 8);
                }
            }
            // Standoffs
            for (x = [-1, 1], y = [-1, 1])
                translate([x * 15.5, y * 15.5])
                    cylinder(d = 5, h = 8);
        }
        
        // Mounting holes
        for (x = [-1, 1], y = [-1, 1])
            translate([x * 15.5, y * 15.5, -1])
                cylinder(d = bolt_d, h = 12);
    }
}

// ============================================================
// ASSEMBLY PREVIEW
// ============================================================
module assembly() {
    // Fuselage
    color("#5577aa", 0.9) fuselage();
    
    // 4 arms in X
    color("#dd7733", 0.9)
        for (i = [0:3]) {
            angle = i * 90 + 45;
            rotate([0, 0, angle])
                translate([body_diam/2 - 1, 0, body_len * 0.45 - arm_thick/2 - 0.25])
                    arm();
        }
    
    // Stack mount on top
    color("#ccbb33", 0.9)
        translate([0, 0, body_len - 20])
            stack_mount();
}

// ============================================================
// RENDER
// ============================================================
which_part = "assembly";

if (which_part == "fuselage")    fuselage();
if (which_part == "arm")         arm();
if (which_part == "stack_mount") stack_mount();
if (which_part == "assembly")    assembly();
