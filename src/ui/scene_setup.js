import * as THREE from 'https://unpkg.com/three@0.160.0/build/three.module.js';

let scene, camera, renderer;
let players = []; 
let ball; // THE BALL

export function init3DScene(containerId) {
    const container = document.getElementById(containerId);

    // 1. SCENE
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x222222);

    // 2. CAMERA
    camera = new THREE.PerspectiveCamera(45, container.clientWidth / container.clientHeight, 0.1, 1000);
    camera.position.set(0, 65, 45); 
    camera.lookAt(0, 0, 0);

    // 3. RENDERER
    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(container.clientWidth, container.clientHeight);
    container.appendChild(renderer.domElement);

    // 4. LIGHTS
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.6);
    scene.add(ambientLight);
    
    const dirLight = new THREE.DirectionalLight(0xffffff, 0.8);
    dirLight.position.set(10, 50, 20);
    scene.add(dirLight);

    // 5. FLOOR
    const floorGeo = new THREE.PlaneGeometry(94, 50); 
    const floorMat = new THREE.MeshStandardMaterial({ color: 0xcfb997, roughness: 0.8 }); 
    const floor = new THREE.Mesh(floorGeo, floorMat);
    floor.rotation.x = -Math.PI / 2; 
    scene.add(floor);

    const lineGeo = new THREE.PlaneGeometry(0.5, 50);
    const lineMat = new THREE.MeshBasicMaterial({ color: 0xffffff });
    const centerLine = new THREE.Mesh(lineGeo, lineMat);
    centerLine.rotation.x = -Math.PI / 2;
    centerLine.position.y = 0.05;
    scene.add(centerLine);

    // 6. THE BASKETBALL [NEW]
    const ballGeo = new THREE.SphereGeometry(0.6, 32, 32); // Radius 0.6
    const ballMat = new THREE.MeshStandardMaterial({ color: 0xff6d00, roughness: 0.4 }); // Orange
    ball = new THREE.Mesh(ballGeo, ballMat);
    ball.position.set(0, 100, 0); // Hide initially
    scene.add(ball);

    animate();
}

function animate() {
    requestAnimationFrame(animate);
    players.forEach(p => {
        if (p.label) p.label.lookAt(camera.position);
    });
    renderer.render(scene, camera);
}

export function clearPlayers() {
    players.forEach(p => {
        scene.remove(p.mesh);
        scene.remove(p.label);
    });
    players = [];
}

// [NEW] MOVE BALL FUNCTION
export function updateBallPosition(x, z) {
    if (ball) {
        // Y=4 puts it roughly at chest height / mid-air
        ball.position.set(x, 4, z);
    }
}

// ... (Keep the rest of your addPlayerToScene / TextSprite code below) ...
// (Paste the Formation Logic from the previous step here if you haven't already)
const HOME_SLOTS = [
    { x: -30, z: 0 }, { x: -25, z: 15 }, { x: -25, z: -15 }, { x: -10, z: 10 }, { x: -10, z: -10 }
];
const AWAY_SLOTS = [
    { x: 30, z: 0 }, { x: 25, z: -15 }, { x: 25, z: 15 }, { x: 10, z: -10 }, { x: 10, z: 10 }
];

export function addPlayerToScene(name, pos, heightScale, widthScale, side, index) {
    const geometry = new THREE.CylinderGeometry(0.7 * widthScale, 0.7 * widthScale, 2.5 * heightScale, 16);
    const color = side === 'home' ? 0xd32f2f : 0x1976D2; 
    const material = new THREE.MeshStandardMaterial({ color: color });
    const mesh = new THREE.Mesh(geometry, material);
    
    const slots = side === 'home' ? HOME_SLOTS : AWAY_SLOTS;
    const slot = slots[index] || { x: (side==='home'?-40:40), z: index*5 };

    const jitterX = (Math.random() * 2) - 1;
    const jitterZ = (Math.random() * 2) - 1;

    mesh.position.set(slot.x + jitterX, (2.5 * heightScale) / 2, slot.z + jitterZ);
    scene.add(mesh);

    const label = createTextSprite(name);
    label.position.set(slot.x + jitterX, 2, slot.z + jitterZ + 3); 
    scene.add(label);

    players.push({ name, mesh, label });
}

function createTextSprite(message) {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    canvas.width = 512; 
    canvas.height = 128;
    ctx.font = "Bold 36px Arial"; 
    ctx.fillStyle = "white";
    ctx.textAlign = "center";
    ctx.shadowColor = "black";
    ctx.shadowBlur = 4;
    ctx.lineWidth = 3;
    ctx.strokeText(message, 256, 64);
    ctx.fillText(message, 256, 64);
    const texture = new THREE.CanvasTexture(canvas);
    const material = new THREE.SpriteMaterial({ map: texture, transparent: true });
    const sprite = new THREE.Sprite(material);
    sprite.scale.set(10, 2.5, 1); 
    return sprite;
}